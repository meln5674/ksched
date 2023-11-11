package mongodb_test

import (
	"encoding/json"

	"go.mongodb.org/mongo-driver/bson"

	mongodb "github.com/meln5674/ksched/contrib/ksched-mongodb"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type testObj struct {
	Foo string
}

func reverse(s string) string {
	rs := []rune(s)
	l := len(rs)
	for ix := 0; ix < len(rs)/2; ix++ {
		rs[ix], rs[l-ix-1] = rs[l-ix-1], rs[ix]
	}
	return string(rs)
}

func (t *testObj) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"foo": reverse(t.Foo)})
}

func (t *testObj) UnmarshalJSON(data []byte) error {
	var obj map[string]string
	err := json.Unmarshal(data, &obj)
	if err != nil {
		return err
	}
	t.Foo = reverse(obj["foo"])
	return nil
}

var _ = Describe("BSONWrapped", func() {
	It("should round-trip an object with custom JSON serde logic", func() {
		obj := testObj{Foo: "test123"}
		marshaled, err := bson.Marshal(mongodb.WrapForBSON(&obj))
		Expect(err).ToNot(HaveOccurred())

		var raw map[string]string
		Expect(bson.Unmarshal(marshaled, &raw)).To(Succeed())
		Expect(raw).To(Equal(map[string]string{"foo": "321tset"}))

		var roundTrip testObj
		Expect(bson.Unmarshal(marshaled, mongodb.WrapForBSON(&roundTrip))).To(Succeed())
		Expect(roundTrip).To(Equal(obj))
	})
})
