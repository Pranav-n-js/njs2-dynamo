
module.exports.connection = (aws) => {

  if (!process.env['@njs2/dynamo']) {
    throw new Error ('REQUIRED @njs2-dynamo in environment variable');
  }

  let envVariables = typeof process.env['@njs2/dynamo'] == 'object' ? process.env['@njs2/dynamo'] : JSON.parse(process.env['@njs2/dynamo']);

  if (!envVariables.USE_IAM_ROLE || envVariables.LOCAL_DB) {

    if (!envVariables.AWS_REGION) {
      throw new Error("AWS_REGION must be defined");
    }

    if (!envVariables.AWS_ACCESS_KEY_ID) {
      throw new Error("AWS_ACCESS_KEY_ID must be defined");
    }

    if (!envVariables.AWS_SECRET_ACCESS_KEY) {
      throw new Error("AWS_SECRET_ACCESS_KEY must be defined");
    }
  }

  aws.config.update({
    region: envVariables.AWS_REGION,
    accessKeyId: envVariables.AWS_ACCESS_KEY_ID,
    secretAccessKey: envVariables.AWS_SECRET_ACCESS_KEY,
    endpoint: envVariables.LOCAL_HOST,
  });

}