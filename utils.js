const elasticClient = require('./elastic-config');


const exceptionMappings = [
  'book_now',
  'provider_request_id',
  'flag',
  'ignoreWarning',
  'override_cost_token',
  'ignoreAddressWarning',
  'previousMessageByPatient'
];

const logstashExceptionMappings = [
  'updated_to',
  'book_now',
  'provider_request_id',
  'flag',
  'ignoreWarning',
  'override_cost_token',
  'ignoreAddressWarning',
  'statusCode',
  'Lyft fare'
];


const createIndex = async function createIndex(indexName, fromIndex) {
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
    properties.index_type = { type: 'keyword' };
    Object.entries(mappings).forEach(([key, value]) => {
      if (value.properties) {
        Object.entries(value.properties).forEach(([propKey, propValue]) => {
          if (properties[propKey]) {
            const prevProps = properties[propKey];
            const newProps = { properties: {} };
            if (propValue && (!propValue.type || propValue.type === 'nested') && prevProps.properties && propValue.properties) {
              updateMappingType(propValue.properties, exceptionMappings);
              newProps.properties = { ...prevProps.properties, ...propValue.properties };
              properties[propKey] = newProps;
            } else {
              if(exceptionMappings.includes(propKey)) {
                propValue.type = 'text';
              }
              properties[propKey] = propValue;
            }
          } else {
            if(exceptionMappings.includes(propKey)) {
              propValue.type = 'text';
            }
            if(!propValue.type || propValue.type === 'nested') {
              updateMappingType(propValue.properties, exceptionMappings)
            }
            properties[propKey] = propValue;
          }
  
        });
      }
    });
    await elasticClient.indices.putMapping({
      index: indexName, type: 'doc', body: { properties, dynamic: false },
    });
}

const createLogstashIndex = async function createLogstashIndex(indexName, fromIndex) {
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
  properties.index_type = { type: 'keyword' };
  Object.entries(mappings).forEach(([key, value]) => {
    if (value.properties) {
      Object.entries(value.properties).forEach(([propKey, propValue]) => {
        if (properties[propKey]) {
          const prevProps = properties[propKey];
          const newProps = { properties: {} };
          if (propValue && (!propValue.type || propValue.type === 'nested') && prevProps.properties && propValue.properties) {
            updateMappingType(propValue.properties, logstashExceptionMappings);
            newProps.properties = { ...prevProps.properties, ...propValue.properties };
            properties[propKey] = newProps;
          } else {
            if(logstashExceptionMappings.includes(propKey)) {
              propValue.type = 'text';
            }
            properties[propKey] = propValue;
          }
        } else {
          if(logstashExceptionMappings.includes(propKey)) {
            propValue.type = 'text';
          }
          if(!propValue.type || propValue.type === 'nested') {
            updateMappingType(propValue.properties, logstashExceptionMappings)
          }
          properties[propKey] = propValue;
        }

      });
    }
  });
  await elasticClient.indices.putMapping({
    index: indexName, type: 'doc', body: { properties, dynamic: false },
  });
}
  
function updateMappingType(obj, exceptionMappings) {
    for (let mappingKey in obj) {
        if (!obj.hasOwnProperty(mappingKey)) continue;
        if (typeof obj[mappingKey] == 'object' && obj[mappingKey].properties) {
          updateMappingType(obj[mappingKey].properties, exceptionMappings)
        }
        if (exceptionMappings.includes(mappingKey)) {
            obj[mappingKey] = {type: 'text'};
        }
    }
}
module.exports = {createIndex, createLogstashIndex};