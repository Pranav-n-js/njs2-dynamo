const { BILLING_MODE, KEY_TYPE, ATTRIBUTE_TYPE } = require('../constants/constant');
const { DataHelper } = require('../helper/dataHelper');

const AWS = require('aws-sdk');

class Schema {
    constructor(name, schema, billingMode = BILLING_MODE.PAY_REQUEST, provisionedThroughput = { readCapacity: 0, writeCapacity: 0 }) {

        /* This is the Table name which is used */
        this.name = name;
        /*
            Schema is defined for future uses. ex -
            KeyName: {
                KeyType: HASH | Range
                AttributeType: S | N | B | BOOL | NULL | M | L | SS | NS | BS
            }
        */
        this.schema = schema;

        /* Billing mode can be either Pay per request or Provisioned */
        if (billingMode !== BILLING_MODE.PAY_REQUEST && billingMode !== BILLING_MODE.PROVISIONED) {
            throw new Error(`Billing mode must be ${Object.values(BILLING_MODE)}`)
        }

        this.billingMode = billingMode;

        if (billingMode === BILLING_MODE.PROVISIONED) {
            /* For provisioned throughput required read (RCU) and write capacity units (WCU)   */
            if (
                !("readCapacity" in provisionedThroughput) ||
                provisionedThroughput.readCapacity === 0 ||
                !("writeCapacity" in provisionedThroughput) ||
                provisionedThroughput.writeCapacity === 0
            ) {
                throw new Error("REQUIRED READ OR WRITE Capacity for billing mode " + billingMode);
            }
        }

        this.provisionedThroughput = {
            ReadCapacityUnits: provisionedThroughput.readCapacity,
            WriteCapacityUnits: provisionedThroughput.writeCapacity
        };
        if (!process.env['@njs2/dynamo']) {
            throw new Error('REQUIRED @njs2-dynamo in environment variable');
        }
        let envVariables = typeof process.env['@njs2/dynamo'] == 'object' ? process.env['@njs2/dynamo'] : JSON.parse(process.env['@njs2/dynamo']);

        this.endpoint = envVariables.LOCAL_HOST;
        this.region = envVariables.region;
    }

    connection() {

        if (!process.env['@njs2/dynamo']) {
            throw new Error('REQUIRED @njs2-dynamo in environment variable');
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

        AWS.config.update({
            region: envVariables.AWS_REGION,
            accessKeyId: envVariables.AWS_ACCESS_KEY_ID,
            secretAccessKey: envVariables.AWS_SECRET_ACCESS_KEY,
            endpoint: envVariables.LOCAL_HOST,
        });
    }

    CreateTable = async () => {
        try {
            AWS.config.update({
                region: "local"
            });
            const DynamoDB = new AWS.DynamoDB({
                endpoint: new AWS.Endpoint('http://localhost:8000')
            });
            // this.connection();
            const KeySchema = [], AttributeDefinitions = [];

            for (const key in this.schema) {
                /* Check if KeyType is present then add HASH|RANGE (Primary Key | sort Key) */
                if (
                    "KeyType" in this.schema[key] &&
                    (
                        this.schema[key].KeyType === KEY_TYPE.PRIMARY_KEY ||
                        this.schema[key].KeyType === KEY_TYPE.RANGE
                    )
                ) {
                    KeySchema.push({
                        AttributeName: key,
                        KeyType: this.schema[key].KeyType
                    });

                    AttributeDefinitions.push({
                        AttributeName: key,
                        AttributeType: this.schema[key].AttributeType
                    });
                }
            }

            const createdData = await DynamoDB.createTable({
                AttributeDefinitions,
                KeySchema,
                BillingMode: this.billingMode,
                ProvisionedThroughput: this.provisionedThroughput,
                TableName: this.name
            }).promise();
            console.log({ createdData });

            let status = "CREATING";
            let data;

            while (status === "CREATING") {
                data = await this.DescribeTable();
                console.log({ data });
                if (data?.TableStatus === "ACTIVE") {
                    console.log("Making status as ACTIVE");
                    status = "ACTIVE"
                }
                await setTimeout(() => { }, 1000);
            }

            return data;
        } catch (error) {
            console.log("Error creating the table", error);
        }
    }

    DescribeTable = async () => {
        AWS.config.update({
            region: "local"
        });
        const DynamoDB = new AWS.DynamoDB({
            endpoint: new AWS.Endpoint('http://localhost:8000')
        });
        this.connection();

        return await DynamoDB.describeTable({
            TableName: this.name
        }).promise();
    }

    InsertItem = async (data) => {
        const items = {};
        AWS.config.update({
            region: "local"
        });
        const DynamoDB = new AWS.DynamoDB({
            endpoint: new AWS.Endpoint(this.endpoint)
        });

        /*
            To insert data just pass column name as key and value is output.
         */
        const item = DataHelper(data, this.schema);
        return await DynamoDB.putItem({
            TableName: this.name,
            Item: item
        }).promise();
    }

    scanItems = async () => {
        
    }
}

module.exports = Schema;