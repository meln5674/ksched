package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"go.mongodb.org/mongo-driver/bson"
	mongodb "go.mongodb.org/mongo-driver/mongo"

	"sigs.k8s.io/controller-runtime/pkg/client"

	ksched "github.com/meln5674/ksched/pkg/archive"
	kschedobj "github.com/meln5674/ksched/pkg/object"
)

type WithID[T any] struct {
	Object *T
	ID     string `json:"_id"`
}

// When serializing/deserializing types with UnmarhsalJSON/MarshalJSON defined, the mongo bson package breaks, and does not unmarshal an identical value as it marshalled.
// As a workaround, we can marshal a value to bytes, then unmarshal it back to an empty interface. This results in the json package using primitive go types like maps and slices,
// which the mongo bson package does in fact round-trip correctly. We likewise need an addition round-trip when unmarshalling to retrieve the bytes representing the original object's
// JSON, which we can then marshal back into the typed structure using its json methods.

// toJSONPrimitives round-trips an object to json bytes and back to its primitive representation (map[string]interface{}, etc)
func toJSONPrimitives(v interface{}) (interface{}, error) {
	marshaled, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var primitives interface{}
	err = json.Unmarshal(marshaled, &primitives)
	if err != nil {
		return nil, err
	}
	return primitives, err
}

// fromJSONPrimitives round-trips a set of json primitives (map[string]interface{}, etc) back to a typed object
func fromJSONPrimitives(primitives, v interface{}) error {
	marshaled, err := json.Marshal(primitives)
	if err != nil {
		return err
	}
	return json.Unmarshal(marshaled, v)
}

type BSONWrapped[T any] struct {
	Inner T
}

func (b BSONWrapped[T]) MarshalBSON() ([]byte, error) {
	primitives, err := toJSONPrimitives(b.Inner)
	if err != nil {
		return nil, err
	}
	return bson.Marshal(primitives)
}

func (b BSONWrapped[T]) UnmarshalBSON(bs []byte) error {
	var primitives bson.M
	err := bson.Unmarshal(bs, &primitives)
	if err != nil {
		return err
	}
	return fromJSONPrimitives(primitives, b.Inner)
}

func WrapForBSON[T any](v T) BSONWrapped[T] {
	return BSONWrapped[T]{Inner: v}
}

type Config struct {
	Database   string
	Collection string
}

type Archiver[O kschedobj.Object, OL kschedobj.ObjectList[O]] struct {
	Client *mongodb.Client
	Config Config
}

var _ = ksched.Archiver[kschedobj.Object, kschedobj.ObjectList[kschedobj.Object]]((*Archiver[kschedobj.Object, kschedobj.ObjectList[kschedobj.Object]])(nil))

func (m *Archiver[O, OL]) ArchiveObject(ctx context.Context, obj O) error {
	_, err := m.Client.
		Database(m.Config.Database).
		Collection(m.Config.Collection).
		InsertOne(ctx, WrapForBSON(obj))
	return err
}

func (m *Archiver[O, OL]) GetObject(ctx context.Context, key client.ObjectKey, obj O) error {
	err := m.Client.
		Database(m.Config.Database).
		Collection(m.Config.Collection).
		FindOne(ctx, bson.D{{Key: "metadata.namespace", Value: key.Namespace}, {Key: "metadata.name", Value: key.Name}}).
		Decode(WrapForBSON(obj))
	if errors.Is(err, mongodb.ErrNoDocuments) {
		return ksched.ErrNotExist
	}
	return err
}

func (m *Archiver[O, OL]) SearchObjects(ctx context.Context, q ksched.Query, objs OL) error {
	pipeline := bson.A{}
	if q.Namespaces != nil {
		pipeline = append(pipeline, SetStage("metadata.namespace", q.Namespaces))
	}
	if q.Names != nil {
		pipeline = append(pipeline, SetStage("metadata.name", q.Names))
	}
	if q.NameSubstring != "" {
		pipeline = append(pipeline, RegexStage("metadata.name", ".*"+regexp.QuoteMeta(q.NameSubstring)+".*"))
	}
	if q.NamePrefix != "" {
		pipeline = append(pipeline, RegexStage("metadata.name", regexp.QuoteMeta(q.NameSubstring)+".*"))
	}
	if q.NameSuffix != "" {
		pipeline = append(pipeline, RegexStage("metadata.name", ".*"+regexp.QuoteMeta(q.NameSubstring)))
	}
	if q.Labels.MatchLabels != nil {
		return fmt.Errorf("Labels not yet supported yet")
	}
	if q.Labels.MatchExpressions != nil {
		return fmt.Errorf("Labels not yet supported yet")
	}

	cursor, err := m.Client.
		Database(m.Config.Database).
		Collection(m.Config.Collection).
		Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	objs.Reset(0)
	for cursor.Next(ctx) {
		obj := objs.AppendEmpty()
		err := cursor.Decode(WrapForBSON(obj))
		if err != nil {
			return err
		}
	}
	return nil
}

func SetStage(field string, items []string) bson.D {
	bsonItems := make(bson.A, len(items))
	for ix, item := range items {
		bsonItems[ix] = item
	}
	return bson.D{
		{Key: "$match", Value: bson.D{
			{Key: field, Value: bson.D{
				{Key: "$in", Value: bsonItems},
			}},
		}},
	}
}

func RegexStage(field string, pattern string) bson.D {
	return bson.D{
		{Key: "$match", Value: bson.D{
			{Key: field, Value: bson.D{
				{Key: "$regex", Value: pattern},
			}},
		}},
	}
}

func EqualsStage(field string, value any) bson.D {
	return bson.D{
		{Key: "$match", Value: bson.D{
			{Key: field, Value: bson.D{
				{Key: "$equal", Value: value},
			}},
		}},
	}
}
