module.exports.generateUpdateExpression = (updateValue, updateKey, schema, initialKey = null) => {

    let updateExpression = 'SET ';
    let value = {}, key = {};

    // Update only if the length of keys and values are same.
    if (updateValue.length == updateKey.length) {

        // Iterate over each key.
        updateKey.forEach((upKey, index) => {

            // If Key is not present then don't update.
            if (schema[upKey]) {

                // For avoiding error on reserved keywords we are converting keys with # and values with :.
                let currKey = ""
                if (initialKey) currKey = '#' + initialKey + `k${upKey}${index}`;
                else currKey = `#k${upKey}${index}`;

                updateExpression += ` ${currKey} = :v${upKey}${index}`;

                if (updateKey[index + 1]) {
                    updateExpression += ', '
                }
                value[`:v${upKey}${index}`] = {[schema[upKey].AttributeType]: updateValue[index]};
                key[currKey] = upKey;
            }
        })
    }
    return {
        UpdateExpression: updateExpression,
        ExpressionAttributeNames: key,
        ExpressionAttributeValues: value
    }
}
