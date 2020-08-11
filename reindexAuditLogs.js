
const elasticClient = require('./elastic-config');
const redisClient = require('./redis');

const MAX_DOC_COUNT = 50;

async function callBulkAPI(elindex) {
    const bulkResponse = await elasticClient.bulk({
      body: elindex,
      timeout: '5m',
    });
    if(bulkResponse.errors) {
      console.error(JSON.stringify(bulkResponse));
    }
    return bulkResponse;
}

async function bulkIndexAuditLogs(rideId, auditLogs, rideRange) {   
    const elindex = [];
    auditLogs.forEach((value) => {
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
            ride_id: rideId,
          },
      });
    }
    redisClient.set(`elasticCleanUp:${rideRange.index}:rideId`, rideId);
}

async function reindexAuditLogs(rideRange) {
    for (let rideIndex = rideRange.startRideId; rideIndex < rideRange.endRideId; rideIndex++) {
      const auditLogQuery = {
        index: 'rides',
        size: MAX_DOC_COUNT,
        scroll: '1m',
        body: {
              query: {
                match: {
                  'ride_details_id': rideIndex,
                },  
              },
        },
      };
      let { hits: { total: count, hits: auditLogs }, _scroll_id: scrollId } = await elasticClient.search(auditLogQuery);
      if(auditLogs && auditLogs.length) {
        await bulkIndexAuditLogs(rideIndex, auditLogs, rideRange);
      }
      while(scrollId && count > MAX_DOC_COUNT) {
        const { hits: { hits: auditLogs },  _scroll_id: newScrollId } = await elasticClient.scroll({scrollId});
        scrollId = newScrollId;
        if (auditLogs && auditLogs.length) {
          await bulkIndexAuditLogs(rideIndex, auditLogs, rideRange);
        }
      }
      redisClient.set(`elasticCleanUp:${rideRange.index}:rideId`, rideIndex);
    }
}

async function createIndex(indexName, fromIndex) {
  await elasticClient.indices.create({
    index: indexName,
    body: {
      settings: {
        index: {
          'mapping.total_fields.limit': 70000,
          number_of_shards: 1,
          number_of_replicas: 0,
          refresh_interval: -1,
        },
      },
    },
  });
  const result = await elasticClient.indices.getMapping({ index: fromIndex });
  const { mappings } = result[fromIndex];
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
}

async function reindexJob() {
    try {
      const [ rangeIndex, startRideId, endRideId ] =  process.argv.slice(2);
      let lastProcessedRideId = await redisClient.get(`elasticCleanUp:${rangeIndex}:rideId`);
      const rideRangeEvent = {
        index: rangeIndex,
        startRideId: parseInt(startRideId, 10),
        endRideId: parseInt(endRideId, 10)
      };
      const isIndexExists = await elasticClient.indices.exists({index: rangeIndex})
      if(!isIndexExists) {
        await createIndex(rangeIndex, 'rides');
      }
      if(parseInt(lastProcessedRideId, 10)) {
        rideRangeEvent.startRideId = parseInt(lastProcessedRideId, 10) + 1;
      }
      if(rideRangeEvent.startRideId < parseInt(rideRangeEvent.endRideId, 10)) {
        await reindexAuditLogs(rideRangeEvent);
      }
      } catch(err) {
        console.error(err);
      }
}
reindexJob();