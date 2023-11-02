const constants = {};
constants.BILLING_MODE = {
    PAY_REQUEST: 'PAY_PER_REQUEST',
    PROVISIONED: 'PROVISIONED'
};
constants.KEY_TYPE = {
    'PRIMARY_KEY': 'HASH',
    'RANGE': 'RANGE'
}
constants.ATTRIBUTE_TYPE = {
    STRING: 'S',
    NUMBER: 'N',
    Binary: 'B',
    BOOL: 'BOOL',
    NULL: 'NULL',
    MAP: 'M',
    LIST: 'L',
    STRING_SET: 'SS',
    NUMBER_SET: 'NS',
    BINARY_SET: 'BS'
}

module.exports = constants;