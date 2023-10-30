const { getConnection } = require('../helper/dbConnection');

module.exports = async function createSchema(newSchema) {
  try {
    if (!newSchema) return null;

    const conn = await getConnection();
    const dynamoose = require('dynamoose');
    return new dynamoose.Schema(newSchema);
  } catch (error) {
    console.log('Error: njs2-dynamo: createSchema', error);
    throw new Error(error);
  }
}