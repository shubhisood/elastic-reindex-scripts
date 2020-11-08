const moment = require('moment');
const elasticClient = require('./elastic-config');
const redisClient = require('./redis');
const utils = require('./newUtils');

const INDEX_NAME = 'vendor-portal';
const MAX_DOC_COUNT = 50;
async function callBulkAPI(elindex) {
  if (elindex.length) {
    const bulkResponse = await elasticClient.bulk({
      body: elindex,
      timeout: '5m',
      filterPath:'items.index._id,errors'
    });
    if(bulkResponse.errors) {
      console.error('ERROR RESPONSE_____________________________');
      console.error(JSON.stringify(bulkResponse));
      throw new Error(JSON.stringify(bulkResponse));
    }
    return bulkResponse;
  }
  return null;
}

async function bulkIndexLogstashLogs(logstashLogs, indexName, startMoment) {
    const startDate = startMoment.format('YYYY-MM-DD HH:mm:ss.SSSZ');
    const { _source: { createdAt : latestLogCreatedAt }} = logstashLogs.reduce((prevVal, currentVal) => {
        const { _source: { createdAt: prevCreatedAt }} = prevVal;
        const { _source: { createdAt: currentCreatedAt }} = currentVal
        const diffValue = moment(prevCreatedAt).diff(moment(currentCreatedAt));
        if(diffValue < 0) {
            return currentVal;
        } 
        return prevVal;
    }, { _source: { createdAt: startDate }});
    try {
        const elindex = [];
        logstashLogs.forEach((value) => {
        const {_source: source, _type: type, _id: id} = value;
        elindex.push({
          index: {
          _index: indexName,
          _type: 'doc',
          _id: id
          },
        });
        const logValue = JSON.parse(JSON.stringify(source));
        logValue.index_type = type;
        elindex.push(logValue);
        });
        const retValue = await callBulkAPI(elindex);
        if(retValue) {
          for(let bulkResIndex = 0; bulkResIndex < retValue.items.length; bulkResIndex+=1) {
            if (retValue.items[bulkResIndex].errors) {
                const { index: {_id: logstashId } }  = retValue.items[bulkResIndex]
                elasticClient.index({
                  index: 'elastic_cleanup_vendor_portal_errors',
                  type: 'doc',
                  body: {
                      logstash_id: logstashId,
                      meta: JSON.stringify(retValue.items[bulkResIndex]),
                  },
                });
            }
          }
        }
      return latestLogCreatedAt;
    } catch (err) {
      if(logstashLogs.length) {
        logstashLogs.forEach((value, key) => {
          elasticClient.index({
            index: 'elastic_cleanup_vendor_portal_errors',
            type: 'doc',
            body: {
                logstash_id: value._id,
                meta: JSON.stringify(err),
            },
          });
      });
      } else {
        elasticClient.index({
          index: 'elastic_cleanup_vendor_portal_errors',
          type: 'doc',
          body: {
              meta: JSON.stringify(error),
          },
        });
      }
    }
}

const reindexVendorPortalLogs =  async function VendorPortal(year, month) {
  const startDateInRedis = await redisClient.get(`elasticCleanUp:vendorPortalLastProcessedDate${month}${year}`);
  let startMoment = moment(`${year}-${month}`,'YYYY-MM').startOf('month');
  const endMoment = moment(`${year}-${month}`,'YYYY-MM').endOf('month');
  const monthName = moment().month(month - 1).format('MMMM').toLowerCase();
  const indexName = 'vendor_portal_modified';
  // Check for index existence otherwise create one.
  try {
  const indexExists = await elasticClient.indices.exists({ index: indexName });
  if(!indexExists) {
    await utils.createIndexVendorPortal(indexName, 'vendor-portal');
  }
  console.error(startDateInRedis);
  if(startDateInRedis) {
    startMoment = moment(startDateInRedis);
  }
    const logstashQuery = {
      index: INDEX_NAME,
      scroll:'1m',
      body: {
        size: MAX_DOC_COUNT,
        query: {
          range: {
            createdAt: {
              gte: startMoment.format('YYYY-MM-DDTHH:mm:ss.SSSZ'),
              lte: endMoment.format('YYYY-MM-DDTHH:mm:ss.SSSZ'),
              format: `yyyy-MM-dd'T'HH:mm:ss.SSSZ`,
            },
          },
        },
        sort: [
          {
            createdAt: 'asc',
          },
        ],
      },
    };
    const res = await elasticClient.search(logstashQuery);
    let {  hits: { hits: logstashLogs, total: count }, _scroll_id : scrollId } = res;
      if (logstashLogs && logstashLogs.length) {
        let lastProcessedDate = await bulkIndexLogstashLogs(logstashLogs, indexName, startMoment);
        await redisClient.set(`elasticCleanUp:vendorPortalLastProcessedDate${month}${year}`, lastProcessedDate);
        if(count > MAX_DOC_COUNT) {
          while(scrollId && logstashLogs && logstashLogs.length) {
            const { hits: { hits: logs }, _scroll_id : newScrollId } = await elasticClient.scroll({scroll: '1m', scrollId });
            scrollId = newScrollId;
            if (logs && logs.length) {
              lastProcessedDate = await bulkIndexLogstashLogs(logs, indexName, startMoment); 
            } else {
              break;
            }
            await redisClient.set(`elasticCleanUp:vendorPortalLastProcessedDate${month}${year}`, lastProcessedDate);
          }
        }
    }
  } catch(error) {
    console.error(error);
    throw error;
  }

  console.log('::__reindexVendorPortalLogs finished__::')
}

async function reindexJob() {
  const [year, month ] =  process.argv.slice(2);
   await reindexVendorPortalLogs(year, month);
}
reindexJob();
