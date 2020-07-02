# kafka-connect-transform-regexpextract
Kafka Connect transform for extracting a substring from a field using a regular expression.


## Documentation
This transform supports the following configs

|config   | description  | required  | default value  |
|---|---|---|---|
| `source.field.name`  | Field to search with regexp pattern  | yes  |  none |
| `destination.field.name`  | Field to store pattern's match  |  yes | none  |
| `pattern` | Regular Expression to match | yes | none |
| `occurrence`  | Match occurrence, defaults to the first match  | no | 1 |
| `case.sensitive` | Match case sensitive, defaults to true | no | true |

### Example
Given a record with a value like this:
```$xslt
{
  "myfield": "Cat Cut Cot"
  "id": "abc123"
}
```

Applying this transform to the connector:
```$xslt
transforms=regexpextract
transforms.regexpextract.type=com.github.cjmatta.kafka.connect.transform.RegexpExtract$Value
transforms.regexpextract.source.field.name=myfield
transforms.regexpextract.destination.field.name=matchfield
transforms.regexpextract.pattern="C.t"
transforms.regexpextract.occurrence=2
```

would yeild a record like this:
```$xslt
{
  "myfield": "Cat Cut Cot",
  "id": "abc123",
  "matchfield": "Cut"
}
```
