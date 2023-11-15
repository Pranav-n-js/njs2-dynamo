const { ATTRIBUTE_TYPE } = require('../constants/constant');

module.exports.DataHelper = (data, schema) => {
    const items = {};
    console.log(schema);
    for (const keyName in data) {

        if (!keyName) {
            continue;
        }
        if (!schema[keyName] || !("AttributeType" in schema[keyName])) {
            console.log(`${keyName} Not found in schema`);
            continue;
        }
        // For Boolean convert the value to true | false.
        if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.BOOL) {
            items[keyName] = {
                [schema[keyName].AttributeType]: !!data[keyName]
            }
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.NUMBER) {
            items[keyName] = {
                [schema[keyName].AttributeType]: '' + data[keyName]
            }
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.BINARY) {
            items[keyName] = {
                [schema[keyName].AttributeType]: data[keyName]
            };
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.STRING) {
            items[keyName] = {
                [schema[keyName].AttributeType]: `${data[keyName]}`
            };
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.LIST) {
            const arrData = this.ListHelper(keyName[data])
            items[keyName] = {
                [ATTRIBUTE_TYPE.LIST]: arrData
            }
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.MAP) {

            // If schema is not present then throw error.
            if (!schema[keyName].Schema) {
                console.log("*** Schema is required ***");
                throw new Error("Schema is required for Map attribute Type");
            }
            const mapData = this.DataHelper(data[keyName], schema[keyName].Schema)
            // this.MapHelper(data[keyName], schema[keyName].Schema);
            items[keyName] = {
                [ATTRIBUTE_TYPE.MAP]: mapData
            }
        }

        else if (
            schema[keyName].AttributeType == ATTRIBUTE_TYPE.STRING_SET ||
            schema[keyName].AttributeType == ATTRIBUTE_TYPE.BINARY_SET ||
            schema[keyName].AttributeType == ATTRIBUTE_TYPE.NUMBER_SET
        ) {
            let valueData = []
            for (const val of data[keyName]) {
                valueData.push({
                    [schema[keyName].AttributeType]: `${val}`
                });
            }
        }
    }
    console.log({ items});
    return JSON.parse(JSON.stringify(items));
}

module.exports.ListHelper = (arrData) => {
    const data = [];

    for (const val of arrData) {
        if (typeof val == 'string') {
            data.push({
                [ATTRIBUTE_TYPE.STRING]: val
            });
        } else if (typeof val == 'number') {
            data.push({
                [ATTRIBUTE_TYPE.NUMBER]: '' + val
            });
        }
        else if (typeof val == 'object') {
            data.push({
                [ATTRIBUTE_TYPE.STRING]: JSON.stringify(val)
            });
        }
        else if (val == null || val == undefined) {
            data.push({
                [ATTRIBUTE_TYPE.NULL]: true
            });
        }
        else if (Array.isArray(val)) {
            const arrData = this.ListHelper(val);
            data.push({
                [ATTRIBUTE_TYPE.LIST]: arrData
            });
        }
        else if (typeof val == 'object') {
            const mapData = this.MapHelper(val);
            data.push({
                [ATTRIBUTE_TYPE.MAP]: mapData
            });
        }
    }

    return data;
}

module.exports.MapHelper = (data, schema) => {

    const mapData = {};

    for (const subDocKey in data) {
        if (typeof data[subDocKey] == 'number') {
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.NUMBER]: '' + data[subDocKey]
            }
        }

        else if (typeof data[subDocKey] == 'string') {
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.STRING]: data[subDocKey]
            }
        }

        else if (typeof data[subDocKey] == 'boolean') {
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.BOOL]: (!!data[subDocKey])
            }
        }

        else if (data[subDocKey] == null) {
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.NULL]: true
            }
        }

        else if (Array.isArray(data)) {
            const arrData = this.ListHelper(data);
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.LIST]: arrData
            }
        }

        else if (typeof data[subDocKey] == 'object') {
            const subMapData = this.MapHelper(data[subDocKey]);
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.MAP]: subMapData
            }
        }
    }

    return mapData;
}

module.exports.ExtractDataTypeFromArray = (data) => {
    const items = [];
    for (const value of data) {
        const item = this.ExtractDataType(value);
        items.push(item);
    }
    return items;
}

module.exports.ExtractDataType = (data) => {
    let items = {};

    if (Array.isArray(data)) {
        items = this.ExtractDataTypeFromArray(data);
    }
    else if (typeof data === 'object') {
        items =  {};
        for (const key in data) {
            items[key] = Object.values(data[key])[0]
        }
    }

    return items;
}