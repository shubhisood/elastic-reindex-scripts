
const elasticClient = require('./elastic-config');
const redisClient = require('./redis');
const utils = require('./newUtils');

const MAX_DOC_COUNT = 20;

async function callBulkAPI(elindex) {
    const bulkResponse = await elasticClient.bulk({
      body: elindex,
      timeout: '5m',
      filterPath:'items.index._id,errors'
    });
    if(bulkResponse.errors) {
     throw new Error(JSON.stringify(bulkResponse))
    }
    return bulkResponse;
}

async function bulkIndexLogs(logs, rideRange) {   
    const elindex = [];
    let rideId;
    logs.forEach((value, index) => {
      const { _id: id, _source: source} = value;
      if(index === logs.length - 1) {
        rideId = id;
      }
      elindex.push({
        index: {
          _index: rideRange.index,
          _type: 'doc',
          _id: value._id,
        },
      });
      elindex.push({...source, index_type: 'rideorder'});
    });
    try {
      await callBulkAPI(elindex);
    } catch(err) {
      console.error(err)
      elasticClient.index({
          index: 'elastic_cleanup_rideorder_errors',
          type: 'doc',
          body: {
            ride_id: rideId,
            body: err
          },
      });
    }
    console.error('rideId', rideRange.startRideId);
    redisClient.set(`elasticCleanUp:rideorder:${rideRange.index}:rideId`, rideRange.startRideId);
}

async function reindexRidesData(rideRange) {
      const rideDataQuery = {
        index: 'rides',
        size: MAX_DOC_COUNT,
        scroll: '1m',
        body: {
            "query": {
                "bool": {
                  "must": [
                    {
                       "match": {
                            "_type": "rideorder"
                        }
                    },
                    {
                       "range": {
                         "external_order_id": {
                           "gte": rideRange.startRideId,
                           "lte": rideRange.endRideId
                         }
                       }
                    },
                    {
                      "range": {
                        "createdAt": {
                          "gte": '2020-12-20T06:42:34.444Z',
                          "lte": '2021-01-03T09:42:34.444Z'
                        }
                      }
                   }
                  ]
                }
            },
            "sort": [
              {
                "external_order_id": {
                  "order": "asc"
                }
              }
            ]
        },
      };
      let { hits: { total: count, hits: logs }, _scroll_id: scrollId } = await elasticClient.search(rideDataQuery);
      if(logs && logs.length) {
        await bulkIndexLogs(logs, rideRange);
      }
      while(scrollId && count > MAX_DOC_COUNT) {
        const { hits: { hits: logs },  _scroll_id: newScrollId } = await elasticClient.scroll({scroll: '1m', scrollId});
        scrollId = newScrollId; 
        if (logs && logs.length) {
          await bulkIndexLogs(logs, rideRange);
        } else {
          break;
        }
      }
}

async function reindexJob() {
    try {
      const [ startRideId, endRideId ] =  process.argv.slice(2);
      const rangeIndex = 'rides_modified';
      let lastProcessedRideId = await redisClient.get(`elasticCleanUp:rideorder:${rangeIndex}:rideId`);
      const rideRangeEvent = {
        index: rangeIndex,
        startRideId: parseInt(startRideId, 10),
        endRideId: parseInt(endRideId, 10)
      };
      if(parseInt(lastProcessedRideId, 10)) {
        rideRangeEvent.startRideId = parseInt(lastProcessedRideId, 10) + 1;
      }
      const isIndexExists = await elasticClient.indices.exists({index: rangeIndex})
      if(!isIndexExists) {
        await utils.createIndex(rangeIndex, 'rides');
      }
      console.log('rideRangeEvent',rideRangeEvent)
      if(rideRangeEvent.startRideId < parseInt(rideRangeEvent.endRideId, 10)) {
        await reindexRidesData(rideRangeEvent);
      }
      } catch(err) {
        console.error(err);
      }

      console.log('::__reindexJob finished__::')
}

reindexJob();