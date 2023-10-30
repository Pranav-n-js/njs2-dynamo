module.exports.getConnection = () => {
  try {

    const dynamoose = require('dynamoose');

    let envVariables = typeof process.env['@njs2-dynamo'] == 'object' ? process.env['@njs2-dynamo'] : JSON.parse(process.env['@njs2-dynamo']);

    if (!envVariables) {
      return false;
    }

    const connectionParams = {};

    if (envVariables.LOCAL_DB === true) {
      console.log(true);
      return dynamoose.aws.ddb.local(envVariables.LOCAL_PORT);
    }

    if (envVariables.USE_IAM_ROLE === false) {
      connectionParams = {
        credentials: {
          accessKeyId: envVariables.AWS_ACCESS_KEY_ID,
          secretAccessKey: envVariables.AWS_SECRET_ACCESS_KEY
        },
        region: envVariables.AWS_REGION
      }
      const ddb = new dynamoose.aws.ddb.DynamoDB(connectionParams);
      return dynamoose.aws.ddb.set(ddb);
    }
    return dynamoose;

  } catch (error) {
    console.log('Error: @njs2/dynamo', error);
    throw new Error(error);
  }
}
