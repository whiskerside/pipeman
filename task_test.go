package pipeman

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type variant struct {
	Title     string
	Inventory int
}
type product struct {
	Title    string
	Variants []variant
}

func TestTask_unmarshalPayload(t *testing.T) {
	variant1 := variant{Title: "variant title", Inventory: 0}
	task := NewTask()
	task.Payload = []byte(`{"title":"product title","variants":[{"title":"variant title","inventory":0}]}`)

	var p1 product
	err := task.unmarshalPayload(&p1)
	require.NoError(t, err)
	require.Equal(t, "product title", p1.Title)
	require.Equal(t, variant1, p1.Variants[0])

	task1 := NewTask()
	task1.Payload = []byte(`{"title": 12345}`)
	err = task1.unmarshalPayload(&p1)
	require.Contains(t, err.Error(), "pipeman: invalid task payload")
}

func TestTask_marshalPayload(t *testing.T) {
	task := NewTask()
	payload := []byte(`{"Title":"product title","Variants":[{"Title":"variant title","Inventory":0}]}`)

	err := task.marshalPayload(product{
		Title:    "product title",
		Variants: []variant{{Title: "variant title", Inventory: 0}},
	})
	require.NoError(t, err)
	require.Equal(t, task.Payload, payload)

	task1 := NewTask()
	err = task1.marshalPayload(map[string]interface{}{"foo": make(chan int)})
	require.Error(t, err)
	require.Empty(t, task1.Payload)
}

func TestEnqueueOptions_Validate(t *testing.T) {
	opts := EnqueueOptions{}
	err := opts.Validate()
	require.Error(t, err)
	require.EqualError(t, err, "pipeman: empty namespace")

	opts.Namespace = "ns1"
	err = opts.Validate()
	require.Error(t, err)
	require.EqualError(t, err, "pipeman: empty qname")

	opts.Qname = "queue1"
	err = opts.Validate()
	require.NoError(t, err)
}

func TestDequeueOptions_Validate(t *testing.T) {
	opts := DequeueOptions{}
	err := opts.Validate()
	require.Error(t, err)
	require.EqualError(t, err, "pipeman: empty namespace")

	opts.Namespace = "ns1"
	err = opts.Validate()
	require.Error(t, err)
	require.EqualError(t, err, "pipeman: empty qname")

	opts.Qname = "queue1"
	err = opts.Validate()
	require.Error(t, err)
	require.EqualError(t, err, "pipeman: at should not be zero")

	opts.At = time.Now()
	opts.InvisibleSec = -1
	err = opts.Validate()
	require.Error(t, err)
	require.EqualError(t, err, "pipeman: invisible sec should be >= 0")

	opts.InvisibleSec = 5
	err = opts.Validate()
	require.NoError(t, err)
}

func TestAckOptions_Validate(t *testing.T) {
	opts := AckOptions{}
	err := opts.Validate()
	require.Error(t, err)
	require.EqualError(t, err, "pipeman: empty namespace")

	opts.Namespace = "ns1"
	err = opts.Validate()
	require.Error(t, err)
	require.EqualError(t, err, "pipeman: empty qname")

	opts.Qname = "queue1"
	err = opts.Validate()
	require.NoError(t, err)
}

func TestFindOptions_Validate(t *testing.T) {
	opts := FindOptions{}
	err := opts.Validate()
	require.Error(t, err)
	require.EqualError(t, err, "pipeman: empty namespace")

	opts.Namespace = "ns1"
	err = opts.Validate()
	require.NoError(t, err)
}
