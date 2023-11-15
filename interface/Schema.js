const { BILLING_MODE, KEY_TYPE } = require('../constants/constant');
const { DataHelper, ExtractDataType } = require('../helper/dataHelper');
const { generateUpdateExpression } = require('../helper/expressionHelper');

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

        // KeyType - Hash is considered as Primary key. primary key is used for updates, get, BulkGet
        this.primaryKey = "";
        for (const key in this.schema) {
            if (this.schema[key]?.KeyType === KEY_TYPE.PRIMARY_KEY) {
                this.primaryKey = key;
                break;
            }
        }

        /* Billing mode can be either Pay per request or Provisioned, Throwing error if its nether of these */
        if (billingMode !== BILLING_MODE.PAY_REQUEST && billingMode !== BILLING_MODE.PROVISIONED) {
            throw new Error(`Billing mode must be ${Object.values(BILLING_MODE)}`)
        }

        this.billingMode = billingMode;

        /* For provisioned throughput required read (RCU) and write capacity units (WCU)  else 0 is assigned. */
        if (billingMode === BILLING_MODE.PROVISIONED) {
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

        /* Connection values are being assigned here */
        if (!process.env['@njs2/dynamo']) {
            throw new Error('REQUIRED @njs2-dynamo in environment variable');
        }
        let envVariables = typeof process.env['@njs2/dynamo'] == 'object' ? process.env['@njs2/dynamo'] : JSON.parse(process.env['@njs2/dynamo']);
        this.endpoint = envVariables.LOCAL_HOST;
        this.region = envVariables.AWS_REGION;
    }

    connection() {

        AWS.config.update({
            region: this.region
        });

        const dynamoDBParams = {};

        if (this.endpoint) {
            dynamoDBParams.endpoint = new AWS.Endpoint(this.endpoint);
        }
        const DynamoDB = new AWS.DynamoDB(dynamoDBParams);

        const DynamoDBClient = new AWS.DynamoDB.DocumentClient(dynamoDBParams);

        return { DynamoDB, DynamoDBClient }
    }
    CreateTable = async () => {
        try {

            const { DynamoDB } = this.connection();
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

                if (data?.Table?.TableStatus === "ACTIVE") {
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
        const { DynamoDB } = await this.connection();

        return await DynamoDB.describeTable({
            TableName: this.name
        }).promise();
    }

    Insert = async (data) => {
        try {
            const { DynamoDB } = this.connection();
            const item = DataHelper(data, this.schema);
            return await DynamoDB.putItem({
                TableName: this.name,
                Item: item
            }).promise();
        } catch (error) {
            console.log("Error on InsertItem(): ", error);
            throw error;
        }
    }

    RawInsertItem = async (params) => {
        try {
            const { DynamoDB } = this.connection();
            return await DynamoDB.putItem(params).promise();
        } catch (error) {
            console.log("Error on RawInsertItem()`", error);
            throw error;
        }
    }

    Scan = async (whereEqualClause = {}, otherConditionalClause = {}, limit = 0, lastKey = null) => {
        try {
            const { DynamoDB } = this.connection();

            const params = {
                TableName: this.name
            };
            if (lastKey != null) {
                params.ExclusiveStartKey = lastKey;
            }

            if (limit >= 1) {
                params.Limit = limit;
            }

            const whereKeys = Object.keys(whereEqualClause);
            const conditionalKeys = Object.keys(otherConditionalClause);

            if (whereKeys.length > 0 || conditionalKeys.length > 0) {
                params.FilterExpression = " ";
                params.ExpressionAttributeNames = {};
                params.ExpressionAttributeValues = {};
            }

            for (let i = 0; i < whereKeys.length; i++ ) {
                if (this.schema[whereKeys[i]]) {
                    params.FilterExpression += ` #k${whereKeys[i]} = :v${whereKeys[i]} `;
                    params.ExpressionAttributeNames[`#k${whereKeys[i]}`] = whereKeys[i];
                    params.ExpressionAttributeValues[`:v${whereKeys[i]}`] = whereEqualClause[whereKeys[i]];

                    if (i <= whereKeys.length) {
                        params.FilterExpression += " AND "
                    }
                }
            }

            /* Checking for conditional keys like >, <, !=  */
            for (let i = 0; i < conditionalKeys.length; i++) {
                if (this.schema[conditionalKeys[i]]) {
                    params.FilterExpression += ` #k${conditionalKeys[i]} ${whereEqualClause[conditionalKeys[i]].condition} :v${conditionalKeys[i]} `;
                    params.ExpressionAttributeNames[`#k${conditionalKeys[i]}`] = conditionalKeys[i];
                    params.ExpressionAttributeValues[`:v${conditionalKeys[i]}`] = whereEqualClause[conditionalKeys[i]].value;

                    if (i <= whereKeys.length) {
                        params.FilterExpression += " AND "
                    }
                }
            }

            const data = await DynamoDB.scan(params).promise();
            let items = ExtractDataType(data?.Items);

            return {
                items: items,
                lastKey: data?.LastEvaluatedKey,
                totalCount: data?.Count
            }
        } catch (error) {
            console.log("Error on ScanItems:", error);
            throw error;
        }
    }

    RawScanItem = async (params) => {
        try {
            const { DynamoDBClient } = this.connection();
            return await DynamoDBClient.scan(params).promise();
        } catch (error) {
            console.log("Error on RawScanItem:", error);
            throw error;
        }
    }
    Query = async (whereEqualClause = {}, otherConditionalClause = {}, limit = 0, lastKey = null) => {
        try {
            const { DynamoDB } = this.connection();

            const params = {
                TableName: this.name
            };

            if (lastKey != null) {
                params.ExclusiveStartKey = lastKey;
            }

            if (limit >= 1) {
                params.Limit = limit;
            }

            const whereKeys = Object.keys(whereEqualClause);
            const conditionalKeys = Object.keys(otherConditionalClause);

            if (whereKeys.length > 0 || conditionalKeys.length > 0) {
                params.FilterExpression = " ";
                params.ExpressionAttributeNames = {};
                params.ExpressionAttributeValues = {};
            }

            for (let i = 0; i < whereKeys.length; i++) {
                if (this.schema[whereKeys[i]]) {
                    params.FilterExpression += ` #k${whereKeys[i]} = :v${whereKeys[i]} `;
                    params.ExpressionAttributeNames[`#k${whereKeys[i]}`] = whereKeys[i];
                    params.ExpressionAttributeValues[`:v${whereKeys[i]}`] = whereEqualClause[whereKeys[i]];

                    if (i <= whereKeys.length) {
                        params.FilterExpression += " AND "
                    }
                }
            }
            for (let i = 0; i < conditionalKeys.length; i++) {
                if (this.schema[conditionalKeys[i]]) {
                    params.FilterExpression += ` #k${conditionalKeys[i]} ${whereEqualClause[conditionalKeys[i]].condition} :v${conditionalKeys[i]} `;
                    params.ExpressionAttributeNames[`#k${conditionalKeys[i]}`] = conditionalKeys[i];
                    params.ExpressionAttributeValues[`:v${conditionalKeys[i]}`] = whereEqualClause[conditionalKeys[i]].value;

                    if (i <= whereKeys.length) {
                        params.FilterExpression += " AND "
                    }
                }
            }

            const data = await DynamoDB.query(params).promise();

            const items = ExtractDataType(data?.Items);
            return {
                items: items,
                lastKey: data?.LastEvaluatedKey,
                totalCount: data?.Count
            }
        } catch (error) {
            console.log("Error on QueryItems:", error);
            throw error;
        }
    }

    RawQueryItem = async (params) => {
        try {
            const { DynamoDBClient } = this.connection();
            return await DynamoDBClient.scan(params).promise();
        } catch (error) {
            console.log("Error on RawScanItem:", error);
            throw error;
        }
    }

    BulkGetItem = async (primaryKeys = []) => {
        try {
            const { DynamoDB } = this.connection();
            const params = {
                [this.name]: {}
            };

            Keys = primaryKeys.map(key => {
                return {
                    [`#${this.primaryKey}`]: {
                        [this.schema[this.primaryKey].AttributeType]: key
                    }
                };
            });

            if (Keys.length > 0) {
                params[this.name] = {
                    Keys: Keys
                }
            }
            const data = (await DynamoDB.batchGetItem(params).promise()).Responses[this.name];
            const items = ExtractDataType(data);
            return items;
        } catch (error) {
            console.log("Error on BulkGetItem:", error);
            throw new Error(error);
        }
    }

    RawBulkGetItems = async (params) => {
        try {
            const { DynamoDB } = this.connection();

            return await DynamoDB.batchGetItem(params).promise();
        } catch (error) {
            console.log("Error on RawBulkGetItems:", error);
            throw new Error(error);
        }
    }

    BulkInsert = async (batchInsertData) => {
        try {
            let insertData = [];

            insertData = batchInsertData.map((insertData) => {
                const items = DataHelper(insertData, this.schema);
                return {
                    PutRequest: {
                        Item: items
                    }
                }
            });

            const { DynamoDB } = this.connection();

            let startIndex = 0;
            let endIndex = 25;

            let promise = [];
            while (insertData.length > startIndex) {

                const subData = insertData.slice(startIndex, endIndex);

                promise.push(DynamoDB.batchWriteItem({
                    RequestItems: {
                        [this.name]: subData
                    }
                }).promise());

                startIndex += endIndex + 1;
                endIndex += 25;
            }

            return await Promise.all(promise);
        } catch (error) {
            console.log("Error BatchInsert: ", error);
            throw error;
        }
    }

    RawBatchInsert = async (params) => {
        try {
            const { DynamoDB } = this.connection();

            return await DynamoDB.batchWriteItem(params).promise();
        } catch (error) {
            console.log("Error BatchInsert: ", error);
            throw error;
        }
    }

    Update = async (updateData, Key, otherConditions = { whereEqualClause: {}, otherConditionalClause: {} }) => {
        try {
            const { DynamoDBClient } = this.connection();

            const updateKey = Object.keys(updateData);
            const updateValue = Object.values(updateData);

            if (updateKey.length != updateValue.length) {
                throw new error('INVALID_KEY_VALUE');
            }

            const {
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues
            } = generateUpdateExpression(updateValue, updateKey, this.schema);

            return await DynamoDB.updateItem({
                TableName: this.name,
                Key: {
                    [this.primaryKey]: {
                        [this.schema[ this.primaryKey ].AttributeType]: Key
                    }
                },
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues
            }).promise();
        } catch (error) {
            console.log("Error on UpdateItems: ", error);
            throw error;
        }
    }

    RawUpdate = async (params) => {
        try {
            const { DynamoDBClient } = this.connection();
            return await DynamoDBClient.update(params).promise();
        } catch (error) {
            console.log("Error on RawUpdateItems: ", error);
            throw error;
        }
    }

    Delete = async (key) => {
        try {
            const { DynamoDBClient } = this.connection();
            return DynamoDBClient.delete({
                TableName: this.name,
                Key: key,
            }).promise()
        } catch (error) {
            console.log("Error on DeleteItems: ", error);
            throw error;
        }
    }

    RawDelete = async (params) => {
        try {
            const { DynamoDBClient } = this.connection();
            return DynamoDBClient.delete(params).promise()
        } catch (error) {
            console.log("Error on DeleteItems: ", error);
            throw error;
        }
    }

    Get = async (primaryKeyValues) => {
        try {
            const { DynamoDB } = this.connection();
            const params = {};

            let Keys = primaryKeyValues.map(key => {
                return {
                    [`#${this.primaryKey}`]: {
                        [this.schema[this.primaryKey].AttributeType]: key
                    }
                };
            })

            if (Keys.length > 0) {
                params[this.name] = {
                    Keys: Keys
                }
            }
            const data = await DynamoDB.getItem(params).promise();
            const items = ExtractDataType(data.Item);

            return items;
        } catch (error) {
            console.log("Error on Get: ", error);
            throw new Error(error);
        }
    }

    RawGet = async (params) => {
        try {
            const { DynamoDB } = this.connection();
            return await DynamoDB.getItem(params).promise();
        } catch (error) {
            console.log("Error on RawGet: ", error);
            throw new Error(error);
        }
    }
}

module.exports = Schema;