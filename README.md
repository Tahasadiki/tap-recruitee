# tap-recruitee

This is a [Recruitee](https://recruitee.com/) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) from a Recruitee API.

This tap:

- Pulls raw data from [Recruitee](https://recruitee.com/) [API](https://api.recruitee.com/docs/index.html)
- Extracts the following resources:
  - [List-Candidates](https://api.recruitee.com/docs/index.html#candidate)
  - [List-Offers](http://woocommerce.github.io/woocommerce-rest-api-docs/#list-all-orders)
  - [List-Job_boards](http://woocommerce.github.io/woocommerce-rest-api-docs/#list-all-orders)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

For more details checkout [schemas](https://github.com/Tahasadiki/tap-recruitee/tree/master/tap_recruitee/schemas).

## Usage:

### 1. Create Config file from config.json

```
{
    "url": "https://api.recruitee.com/",
    "company_id": "3233",
    "auth_token": "auth_token",
    "start_date": "1900-09-15T15:53:00"
}
```
- the company id and auth token will be needed to connect to Recruitee API
- start date will determine how far back in your data history the tap will go
	- this is only relevant for the initial run, progress afterwards will be bookmarked

### 2. Discover

```
$tap-recruitee --config config.json --discover >> catalog.json
```
- Run the above to discover the data points the tap supports for each of Recruitee's endpoints (currently List-Candidates, List-Offers & List Job_Boards)

### 3. Select Streams

```
    {
       "schema": {
            "properties": {...},
            "type": "object",
            "selected": true
        },
        "stream": "candidate",
        "tap_stream_id": "candidate"
    }
```
- Add ```"selected":true``` within the schema object to select the stream

### 4.Run the tap

```
$tap-recruitee --config config.json --catalog catalog.json
```
### 5.Run tap with target

```
tap-recruitee --config config.json --catalog catalog.json | target-sample --config target-config.json
```
---
