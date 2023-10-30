const createModel = require('./interface/createModel');
const createSchema = require('./interface/createSchema');

class DYNAMOManager {
  /**
   * Database Create Schema
   * @function createSchema
   * @param {Object} collectionStructure
   * @returns {Promise<Number>}
   */
  static createSchema = createSchema();

  /**
   * Database Create Model
   * @function createModel
   * @param {string} collectionName
   * @param {Object} newModel
   * @param {string} collectionNameInDb
   * @returns {Promise<Object>}
   */
  static createModel = createModel();
}

module.exports = DYNAMOManager;