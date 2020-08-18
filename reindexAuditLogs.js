
const elasticClient = require('./elastic-config');
const redisClient = require('./redis');
const utils = require('./utils');

const MAX_DOC_COUNT = 50;


async function callBulkAPI(elindex) {
    const bulkResponse = await elasticClient.bulk({
      body: elindex,
      timeout: '5m',
      filterPath:'items.index._id,errors'
    });
    if(bulkResponse.errors) {
      throw new Error(JSON.stringify(bulkResponse));
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
      const source = {...value._source, index_type: 'audit_log' }
      elindex.push(source);
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
            err: err
          },
      });
    }
}

async function reindexAuditLogs(rideRange) {
    for (let rideIndex = rideRange.startRideId; rideIndex <= rideRange.endRideId; rideIndex++) {
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
        const { hits: { hits: auditLogs },  _scroll_id: newScrollId } = await elasticClient.scroll({scroll: '1m', scrollId});
        scrollId = newScrollId;
        if (auditLogs && auditLogs.length) {
          await bulkIndexAuditLogs(rideIndex, auditLogs, rideRange);
        } else {
          break;
        }
      }
      redisClient.set(`elasticCleanUp:${rideRange.index}:rideId`, rideIndex);
    }
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
        console.error('Creating Index');
        await utils.createIndex(rangeIndex, 'rides');
        console.error('Index Created');
      }
      if(parseInt(lastProcessedRideId, 10)) {
        rideRangeEvent.startRideId = parseInt(lastProcessedRideId, 10) + 1;
      }
      if(rideRangeEvent.startRideId <= parseInt(rideRangeEvent.endRideId, 10)) {
        await reindexAuditLogs(rideRangeEvent);
      }
      } catch(err) {
        console.error(err);
      }
}
reindexJob();