package mongodb_test

import (
	_ "embed"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMongoDB(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MongoDB Suite")
}
