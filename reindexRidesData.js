
const elasticClient = require('./elastic-config.dev');
const redisClient = require('./redis');

const MAX_DOC_COUNT = 20;

async function callBulkAPI(elindex) {
    const bulkResponse = await elasticClient.bulk({
      body: elindex,
      timeout: '5m',
    });
    console.error(JSON.stringify(bulkResponse));
    return bulkResponse;
}

async function bulkIndexLogs(logs, rideRange, rideId) {   
    const elindex = [];
    logs.forEach((value) => {
      elindex.push({
        index: {
          _index: rideRange.index,
          _type: 'doc',
          _id: value._id,
        },
      });
      elindex.push(value._source);
    });
    try {
      await callBulkAPI(elindex);
    } catch(err) {
      console.error(err)
      elasticClient.index({
          index: 'elastic_cleanup_errors',
          type: 'doc',
          body: {
            ride_id: rideId ? rideId : JSON.stringify(elindex),
          },
      });
    }
    redisClient.set(`elasticCleanUp:ridedetails:${rideRange.index}:rideId`, rideId);
}

async function reindexRidesData(rideRange) {
      const rideDataQuery = {
        index: 'rides',
        size: MAX_DOC_COUNT,
        scroll: '10m',
        body: {
            "query": {
                "bool": {
                  "must": [
                    {
                       "match": {
                            "_type": "ride_details"
                        }
                    },
                    {
                       "range": {
                         "id": {
                           "gte": 1,
                           "lte": 100000
                         }
                       }
                    }
                  ]
                }
              }
        },
      };
      let { hits: { total: count, hits: logs }, _scroll_id: scrollId } = await elasticClient.search(rideDataQuery);
      if(logs && logs.length) {
        await bulkIndexLogs(logs, rideRange);
      }
      console.error(count);
      while(scrollId && count > MAX_DOC_COUNT) {
       const resp = await elasticClient.scroll({scrollId});
       let newScrollId;
        scrollId = newScrollId;
        console.error('scrolling' , JSON.stringify(resp));
        if (logs && logs.length) {
          await bulkIndexLogs(logs, rideRange);
        }
      }
     // redisClient.set(`elasticCleanUp:ridedetails:${rideRange.index}:rideId`, rideIndex);
}

async function createIndex(indexName, fromIndex) {
  console.error('info', `Creating index with name ${indexName}`);
  await elasticClient.indices.create({
    index: indexName,
    body: {
      settings: {
        index: {
          'mapping.total_fields.limit': 50000,
          number_of_shards: 1,
          number_of_replicas: 0,
          refresh_interval: -1,
        },
      },
    },
  });
  const result = await elasticClient.indices.getMapping({ index: fromIndex });
  console.error('info', 'got result', result);
  const { mappings } = result[fromIndex];
  console.error('info', 'mappings', mappings);
  const properties = {};
  properties.type = { type: 'keyword' };
  Object.entries(mappings).forEach(([key, value]) => {
    if (value.properties) {
      Object.entries(value.properties).forEach(([propKey, propValue]) => {
        if (properties[propKey]) {
          const prevProps = properties[propKey];
          const newProps = { properties: {} };
          if (propValue && (!propValue.type || propValue.type === 'nested') && prevProps.properties && propValue.properties) {
            newProps.properties = { ...prevProps.properties, ...propValue.properties };
            properties[propKey] = newProps;
          } else {
            properties[propKey] = { ...prevProps, ...propValue };
          }
        } else {
          properties[propKey] = propValue;
        }
      });
    }
  });
  await elasticClient.indices.putMapping({
    index: indexName, type: 'doc', body: { properties, dynamic: false },
  });
  console.error('info', 'After putting mappings');
}

async function reindexJob() {
    try {
      const [ rangeIndex, startRideId, endRideId ] =  process.argv.slice(2);
      let lastProcessedRideId = await redisClient.get(`elasticCleanUp:ridedetails:${rangeIndex}:rideId`);
      const rideRangeEvent = {
        index: rangeIndex,
        startRideId: parseInt(startRideId, 10),
        endRideId: parseInt(endRideId, 10)
      };
      const isIndexExists = await elasticClient.indices.exists({index: rangeIndex})
      console.error('indexExists', isIndexExists)
      if(!isIndexExists) {
        await createIndex(rangeIndex, 'rides');
      }
      console.error('rideRangeEvent', rideRangeEvent)
      if(parseInt(lastProcessedRideId, 10)) {
        rideRangeEvent.startRideId = parseInt(lastProcessedRideId, 10) + 1;
      }
      console.error('rideRangeEvent', rideRangeEvent)
      if(rideRangeEvent.startRideId < parseInt(rideRangeEvent.endRideId, 10)) {
        console.error('reindex audit logs called up')
        await reindexRidesData(rideRangeEvent);
      }
      } catch(err) {
        console.error(err);
      }
}

reindexJob();