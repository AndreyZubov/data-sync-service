package salesforce

import (
	"context"
	"fmt"
	"net/http"
)

// Field describes one field of an object.
type Field struct {
	// Name is the field's API name, e.g. "BillingCity".
	Name string `json:"name"`
	// Type is the Salesforce field type, e.g. "string", "boolean",
	// "datetime", or "address".
	Type string `json:"type"`
	// DeprecatedAndHidden marks fields Salesforce keeps only for
	// compatibility; they cannot be queried.
	DeprecatedAndHidden bool `json:"deprecatedAndHidden"`
}

// ObjectDescription is the subset of an object's describe result the service
// uses.
type ObjectDescription struct {
	Name      string  `json:"name"`
	Queryable bool    `json:"queryable"`
	Fields    []Field `json:"fields"`
}

// HasField reports whether the object has a field with the given API name.
func (d *ObjectDescription) HasField(name string) bool {
	for _, f := range d.Fields {
		if f.Name == name {
			return true
		}
	}
	return false
}

// Describe returns the metadata of object, which must be a valid API name.
func (c *Client) Describe(ctx context.Context, object string) (*ObjectDescription, error) {
	var desc ObjectDescription
	if err := c.DoJSON(ctx, http.MethodGet, "/sobjects/"+object+"/describe", nil, &desc); err != nil {
		return nil, fmt.Errorf("describe %s: %w", object, err)
	}
	return &desc, nil
}
