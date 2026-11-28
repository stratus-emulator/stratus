package sqs

import (
	"fmt"
	"slices"
	"strings"
)

// listElements names the element each item of a JSON list has in the XML form.
var listElements = map[string]string{
	"Messages":  "Message",
	"QueueUrls": "QueueUrl",
	"queueUrls": "QueueUrl",
	"Failed":    "BatchResultErrorEntry",
	"Results":   "ListMessageMoveTasksResultEntry",
}

// pairElements are the JSON maps that the XML form writes as a list of
// elements with a name and a value: JSON name, element, name of the key element.
var pairElements = map[string]struct{ element, key string }{
	"Attributes":        {"Attribute", "Name"},
	"MessageAttributes": {"MessageAttribute", "Name"},
	"Tags":              {"Tag", "Key"},
}

// writeFields writes the members of a result as child elements, in name order.
func writeFields(b *strings.Builder, action string, fields map[string]any) {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		switch v := fields[key].(type) {
		case []any:
			name := listElements[key]
			if key == "Successful" {
				name = action + "ResultEntry"
			}
			for _, item := range v {
				writeElement(b, action, name, item)
			}
		case map[string]any:
			if pair, ok := pairElements[key]; ok {
				writePairs(b, action, pair.element, pair.key, v)
				continue
			}
			writeElement(b, action, key, v)
		default:
			writeElement(b, action, key, v)
		}
	}
}

func writePairs(b *strings.Builder, action, element, keyName string, pairs map[string]any) {
	names := make([]string, 0, len(pairs))
	for name := range pairs {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		b.WriteString("<" + element + "><" + keyName + ">" + escape(name) + "</" + keyName + ">")
		writeElement(b, action, "Value", pairs[name])
		b.WriteString("</" + element + ">")
	}
}

func writeElement(b *strings.Builder, action, name string, value any) {
	b.WriteString("<" + name + ">")
	if fields, ok := value.(map[string]any); ok {
		writeFields(b, action, fields)
	} else {
		b.WriteString(escape(fmt.Sprint(value)))
	}
	b.WriteString("</" + name + ">")
}
