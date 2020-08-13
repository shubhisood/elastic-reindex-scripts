const moment = require('moment');
const elasticClient = require('./elastic-config');
const redisClient = require('./redis');

const INDEX_NAME = 'logstash';
const MAX_DOC_COUNT = 20;
async function callBulkAPI(elindex) {
  if (elindex.length) {
    const bulkResponse = await elasticClient.bulk({
      body: elindex,
      timeout: '5m',
    });
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
                console.error('got error');
                elasticClient.index({
                  index: 'elastic_cleanup_logstash_errors',
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
      console.error('handled error logged in elastic for this batch');
      throw err;
    }
}
async function createIndex(indexName) {
    console.log('info', `Creating index with name ${indexName}`);
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
    const result = await elasticClient.indices.getMapping({ index: INDEX_NAME });
    console.log('info', 'got result', result);
    const { mappings } = result[INDEX_NAME];
    console.log('info', 'mappings', mappings);
    const properties = {};
    properties.index_type = { type: 'keyword' };
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
    console.log('info', 'After putting mappings');
}

const reindexLogstashLogs =  async function reindexLogstash(year, month) {
  const startDateInRedis = await redisClient.get(`elasticCleanUp:logstash${month}${year}`);
  let startMoment = moment(`${year}-${month}`,'YYYY-MM').startOf('month');
  const endMoment = moment(`${year}-${month}`,'YYYY-MM').endOf('month');
  const monthName = moment().month(month - 1).format('MMMM').toLowerCase();
  const indexName = `${INDEX_NAME}_${monthName.toString()}_${year}`;
  // Check for index existence otherwise create one.
  const indexExists = await elasticClient.indices.exists({ index: indexName });
  if(!indexExists) {
    await createIndex(indexName);
  }
  if(startDateInRedis) {
    startMoment = moment(startDateInRedis);
  }
  let logstashLogs = [];

  try {
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
            await redisClient.set(`elasticCleanUp:logstashLastProcessedDate${month}${year}`, lastProcessedDate);
            if(count > MAX_DOC_COUNT) {
              while(scrollId && logstashLogs && logstashLogs.length) {
                ({ hits: { hits: logstashLogs }, _scroll_id : newScrollId } = await elasticClient.scroll({scroll: '1m', scrollId }));
                scrollId = newScrollId;
                if (logstashLogs && logstashLogs.length) {
                  lastProcessedDate = await bulkIndexLogstashLogs(logstashLogs, indexName, startMoment); 
                } else {
                  break;
                }
                await redisClient.set(`elasticCleanUp:logstashLastProcessedDate${month}${year}`, lastProcessedDate);
              }
            }
        }
        console.error('Job completed');  
        return;
  } catch(error) {
    console.error(error);
    if(logstashLogs.length) {
      logstashLogs.forEach((value, key) => {
        elasticClient.index({
          index: 'elastic_cleanup_logstash_errors',
          type: 'doc',
          body: {
              logstash_id: value._id,
              meta: JSON.stringify(error),
          },
        });
    });
    } else {
      elasticClient.index({
        index: 'elastic_cleanup_logstash_errors',
        type: 'doc',
        body: {
            meta: JSON.stringify(error),
        },
      });
    }
  }
}
async function reindexJob() {
  const [year, month ] =  process.argv.slice(2);
   await reindexLogstashLogs(year, month);
}
reindexJob();
