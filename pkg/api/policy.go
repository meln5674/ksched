package api

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"text/template"

	"github.com/golang-jwt/jwt/v4"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A TokenPolicy determines a set of Roles and ClusterRoles from a JWT
type TokenPolicy interface {
	GetRoles(token *jwt.Token) (*RequestRoles, error)
}

// TemplatePolicy is a golang template that should return zero or more Roles or
// ClusterRoles based on a JWT, and the API will act as if that user had those
// roles when accessing k8s and the scan archive
type TemplatePolicy struct {
	// Template is the template to execute to produce YAML-formatted (Cluster)Roles
	Template *template.Template
	// ExtraVars are extra variables to include in the template context
	ExtraVars interface{}
	Decoder   runtime.Decoder
}

type TokenPolicyContext struct {
	Token     interface{}
	ExtraVars interface{}
}

func (t *TemplatePolicy) GetRoles(token *jwt.Token) (*RequestRoles, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	err := t.Template.Execute(buf, TokenPolicyContext{
		Token:     token.Claims,
		ExtraVars: t.ExtraVars,
	})
	if err != nil {
		return nil, err
	}
	reader := yaml.NewYAMLReader(bufio.NewReader(buf))
	req := &RequestRoles{
		VirtualRoles:        make([]rbacv1.Role, 0),
		VirtualClusterRoles: make([]rbacv1.ClusterRole, 0),
	}
	for {
		next, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(bytes.TrimSpace(next)) == 0 {
			continue
		}
		roleObj, _, err := t.Decoder.Decode(next, nil, nil)
		if err != nil {
			return nil, err
		}
		role, isRole := roleObj.(*rbacv1.Role)
		clusterRole, isClusterRole := roleObj.(*rbacv1.ClusterRole)
		if !isRole && !isClusterRole {
			return nil, fmt.Errorf("Unexpected object type in policy: %#v", roleObj)
		}
		if isRole {
			req.VirtualRoles = append(req.VirtualRoles, *role)
		}
		if isClusterRole {
			req.VirtualClusterRoles = append(req.VirtualClusterRoles, *clusterRole)
		}
	}
	return req, nil
}

type RequestRoles struct {
	VirtualRoles        []rbacv1.Role
	VirtualClusterRoles []rbacv1.ClusterRole
}

func StringSliceHas(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}
func StringSliceHasWildcard(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == "*" {
			return true
		}
		if item == needle {
			return true
		}
	}
	return false
}

func RuleAllows(rule *rbacv1.PolicyRule, gvr schema.GroupVersionResource, name string, verb string) bool {
	// Group, Version, and Resource must match or have a wildcard
	// list and deletecollection do not consider resoruceNames, as they apply to the entire collection
	// Other verbs must either match a resource name, or no resource names must be specified
	isMultiVerb := verb == "list" || verb == "deletecollection"
	return StringSliceHasWildcard(rule.Verbs, verb) &&
		StringSliceHasWildcard(rule.APIGroups, gvr.Group) &&
		StringSliceHasWildcard(rule.Resources, gvr.Resource) &&
		(isMultiVerb || len(rule.ResourceNames) == 0 || (name != "" && StringSliceHas(rule.ResourceNames, name)))
}

func RulesAllow(rules []rbacv1.PolicyRule, gvr schema.GroupVersionResource, name string, verb string) bool {
	for _, rule := range rules {
		if RuleAllows(&rule, gvr, name, verb) {
			return true
		}
	}
	return false
}

func (r *RequestRoles) Authorize(gvr schema.GroupVersionResource, key client.ObjectKey, verb string) bool {
	if key.Namespace == "" {
		for _, role := range r.VirtualClusterRoles {
			if RulesAllow(role.Rules, gvr, key.Name, verb) {
				return true
			}
		}
	} else {
		for _, role := range r.VirtualRoles {
			if role.Namespace != key.Namespace {
				continue
			}
			if RulesAllow(role.Rules, gvr, key.Name, verb) {
				return true
			}
		}
		for _, role := range r.VirtualClusterRoles {
			if RulesAllow(role.Rules, gvr, key.Name, verb) {
				return true
			}
		}
	}
	return false
}
