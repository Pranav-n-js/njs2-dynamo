const { getConnection } = require('../helper/dbConnection');

module.exports = async function createModel(collectionName, newSchema) {
  try {
    if (!colllectionName && !newSchema) {
      return null;
    }

    await getConnection();
    const dynamoose = require('dynamoose');
    return dynamoose.model(collectionName, newSchema, { useDocumentClient: false, saveUnknown: true });
  } catch (error) {
    console.log('Error: njs2-dynamo: createModel', error);
    throw new Error(error);
  }
}