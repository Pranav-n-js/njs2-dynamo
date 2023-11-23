const { BILLING_MODE, KEY_TYPE, UPDATE_TYPE } = require('../constants/constant');
const { DataHelper, ExtractDataType } = require('../helper/dataHelper');
const { generateUpdateExpression, generateWhereCondition } = require('../helper/expressionHelper');

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
        if (!process.env['DYNAMO']) {
            throw new Error('REQUIRED DYNAMO in environment variable');
        }
        let envVariables = typeof process.env['DYNAMO'] == 'object' ? process.env['DYNAMO'] : JSON.parse(process.env['DYNAMO']);
        this.endpoint = envVariables.LOCAL_HOST;
        this.region = envVariables.AWS_REGION;
        this.accessKeyId = envVariables.AWS_ACCESS_KEY_ID;
        this.secretAccessKey = envVariables.AWS_SECRET_ACCESS_KEY;
        this.isAutoTableCreated = false;
    }

    connection() {
        if (!this.region) {
            throw new Error ('Required Region');
        }

        if (!this.accessKeyId) {
            throw new Error('Required Access Key ID');
        }

        if (!this.secretAccessKey) {
            throw new Error('Required Secret Access Key ID');
        }

        AWS.config.update({
            region: this.region,
            accessKeyId: this.accessKeyId,
            secretAccessKey: this.secretAccessKey,
        });

        const dynamoDBParams = {};

        if (this.endpoint) {
            dynamoDBParams.endpoint = new AWS.Endpoint(this.endpoint);
        }
        const DynamoDB = new AWS.DynamoDB(dynamoDBParams);

        const DynamoDBClient = new AWS.DynamoDB.DocumentClient(dynamoDBParams);

        return { DynamoDB, DynamoDBClient }
    }

    /**
     * @async
     * @description Creates DynamoDB Table
    */
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

            // Only Hash and Range can be used for creating table
            const createdData = await DynamoDB.createTable({
                AttributeDefinitions,
                KeySchema,
                BillingMode: this.billingMode,
                ProvisionedThroughput: this.provisionedThroughput,
                TableName: this.name
            }).promise();

            // Sometimes while creating a table it takes time, so we need to wait till table is created.
            let status = "CREATING";
            let data;

            while (status === "CREATING") {
                data = await this.DescribeTable();

                if (data?.Table?.TableStatus === "ACTIVE") {
                    console.log(`Making ${this.name} status as ACTIVE`);
                    status = "ACTIVE"
                    this.AutoCreateTable = true;
                }
                await setTimeout(() => { }, 1000);
            }

            return data;
        } catch (error) {
            console.log("Error creating the table", new Error(error));
            throw new Error(error);
        }
    }

    /**
     * @description Gets Table information
     */
    DescribeTable = async () => {
        const { DynamoDB } = await this.connection();

        return await DynamoDB.describeTable({
            TableName: this.name
        }).promise();
    }

    Insert = async (data) => {
        try {
            const { DynamoDB } = this.connection();
            /*
                To insert data just pass column name as key and value is output.
             */
            const item = DataHelper(data, this.schema);
            return await DynamoDB.putItem({
                TableName: this.name,
                Item: item
            }).promise();
        } catch (error) {
            console.log("Error on InsertItem(): ", new Error(error));
            throw new Error(error);
        }
    }

    RawInsert = async (params) => {
        try {
            const { DynamoDB } = this.connection();
            return await DynamoDB.putItem(params).promise();
        } catch (error) {
            console.log("Error on RawInsertItem()`", new Error(error));
            throw new Error(error);
        }
    }

    /**
     *
     * @param {{}} whereEqualClause  - KEY of the object should be same as schema key, and value should be equal to value to find. In whereEqualClause AND is used if multiple keys are used.
     * @param {{}} otherConditionalClause - KEY of the object should be equal to schema key. And the value should be an object. Where { condition: <VALUE>, value: <KEY_VALUE> }, Here <VALUE> should be the condition like  <>, >, < etc and <KEY_VALUE> is the value for condition.
     * @param {number} limit
     * @param {String} lastKey
     * @returns
     */
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
            console.log("Error on Scan():", new Error(error));
            throw new Error(error);
        }
    }

    /**
     * @async
     * @param {{}} params
     * @returns
     * @see DynamoDBClient.scan
     */
    RawScan = async (params) => {
        try {
            const { DynamoDBClient } = this.connection();
            return await DynamoDBClient.scan(params).promise();
        } catch (error) {
            console.log("Error on RawScan():", new Error(error));
            throw new Error(error);
        }
    }

    /**
     *
     * @param {{}} whereEqualClause  - KEY of the object should be same as schema key, and value should be equal to value to find. In whereEqualClause AND is used if multiple keys are used.
     * @param {{}} otherConditionalClause - KEY of the object should be equal to schema key. And the value should be an object. Where { condition: <VALUE>, value: <KEY_VALUE> }, Here <VALUE> should be the condition like  <>, >, < etc and <KEY_VALUE> is the value for condition.
     * @param {number} limit
     * @param {String} lastKey
     * @returns
     */
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

            const data = await DynamoDB.query(params).promise();

            const items = ExtractDataType(data?.Items);
            return {
                items: items,
                lastKey: data?.LastEvaluatedKey,
                totalCount: data?.Count
            }
        } catch (error) {
            console.log("Error on Query():", new Error(error));
            throw new Error(error);
        }
    }

    /**
     * @async
     * @param {{}} params
     * @returns
     * @see DynamoDBClient.scan
     */
    RawQuery = async (params) => {
        try {
            const { DynamoDBClient } = this.connection();
            return await DynamoDBClient.scan(params).promise();
        } catch (error) {
            console.log("Error on RawScan():", new Error(error));
            throw new Error(error);
        }
    }

    BulkGet = async (primaryKeys = []) => {
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
            console.log("Error on BulkGet():", new Error(error));
            throw new Error(error);
        }
    }

    RawBulkGet = async (params) => {
        try {
            const { DynamoDB } = this.connection();

            return await DynamoDB.batchGetItem(params).promise();
        } catch (error) {
            console.log("Error on RawBulk():", new Error(error));
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
            console.log("Error BulkInsert(): ",  new Error( error));
            throw new Error(error);
        }
    }

    RawBulkInsert = async (params) => {
        try {
            const { DynamoDB } = this.connection();

            return await DynamoDB.batchWriteItem(params).promise();
        } catch (error) {
            console.log("Error RawBulkInsert(): ",new Error( error));
            throw new Error(error);
        }
    }

    Update = async (updateData, Key, otherConditions = { whereEqualClause: {}, otherConditionalClause: {}}, updateType = UPDATE_TYPE.SET) => {
        try {
            const { DynamoDB } = this.connection();

            const params = {
                TableName: this.name,
                Key: {
                    [this.primaryKey]: {
                        [this.schema[this.primaryKey].AttributeType]: Key
                    }
                },
            };

            const updateKey = Object.keys(updateData);
            const updateValue = Object.values(updateData);

            if (updateKey.length != updateValue.length) {
                throw new error('INVALID_KEY_VALUE');
            }

            const {
                UpdateExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues
            } = generateUpdateExpression(updateValue, updateKey, this.schema, updateType, null);

            params.UpdateExpression = UpdateExpression;
            params.ExpressionAttributeNames = ExpressionAttributeNames;
            params.ExpressionAttributeValues = ExpressionAttributeValues;

            if (
                otherConditions &&
                (
                    otherConditions.whereEqualClause ||
                    otherConditions.otherConditionalClause
                )
            ) {
                const conditionalExpression = generateWhereCondition(this.schema, otherConditions.whereEqualClause, otherConditions.otherConditionalClause);

                if (conditionalExpression.ConditionExpression.length !== 0) {
                    params.ExpressionAttributeNames = {
                        ...params.ExpressionAttributeNames,
                        ...conditionalExpression.ExpressionAttributeNames
                    };
                    params.ExpressionAttributeValues = {
                        ...params.ExpressionAttributeValues,
                        ...conditionalExpression.ExpressionAttributeValues
                    };
                    params.ConditionExpression = conditionalExpression.ConditionExpression;
                }
            }

            return await DynamoDB.updateItem(params).promise();
        } catch (error) {
            console.log("Error on Update(): ", error);
            throw new Error(error);
        }
    }

    RawUpdate = async (params) => {
        try {
            const { DynamoDBClient } = this.connection();
            return await DynamoDBClient.update(params).promise();
        } catch (error) {
            console.log("Error on RawUpdate(): ", new Error(error));
            throw new Error(error);
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
            console.log("Error on Delete(): ", new Error(error));
            throw new Error(error);
        }
    }

    RawDelete = async (params) => {
        try {
            const { DynamoDBClient } = this.connection();
            return DynamoDBClient.delete(params).promise()
        } catch (error) {
            console.log("Error on RawDelete(): ", new Error(error));
            throw new Error(error);
        }
    }

    Get = async (primaryKeyValues) => {
        try {
            const { DynamoDB } = this.connection();
            const params = {
                TableName: this.name
            };

            if (primaryKeyValues) {
                params.Key = {
                    [`${this.primaryKey}`]: {
                        [this.schema[this.primaryKey].AttributeType]: primaryKeyValues
                    }
                }
                // params.ExpressionAttributeNames = {
                //     [`#${this.primaryKey}`]: this.primaryKey
                // }
            }
            const data = await DynamoDB.getItem(params).promise();
            const items = ExtractDataType(data.Item);

            return items;
        } catch (error) {
            console.log("Error on Get(): ", new Error(error));
            throw new Error(error);
        }
    }

    RawGet = async (params) => {
        try {
            const { DynamoDB } = this.connection();
            return await DynamoDB.getItem(params).promise();
        } catch (error) {
            console.log("Error on RawGet() ", new Error(error));
            throw new Error(error);
        }
    }

    AutoCreateTable = async() => {
        try {
            if (!this.isAutoTableCreated){
                const table = await this.DescribeTable();
                if (!table || !table.Table) {
                    await this.CreateTable();
                }
            }
        } catch (error) {
            await this.CreateTable();
        }
    }
}

module.exports = Schema;