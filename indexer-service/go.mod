module indexer-service

go 1.24.4

require (
	github.com/lib/pq v1.10.9
	github.com/segmentio/kafka-go v0.4.49
	github.com/imadbelkat1/hackernews-services v0.0.0
)

replace github.com/imadbelkat1/hackernews-services => ../hackernews-services

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)
