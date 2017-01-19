'use strict';

const http = require('http');
const url = require('url');
const querystring = require('querystring');

const _ = require('lodash');
const { ImplementationError } = require('flora-errors');

const SUPPORTED_FILTERS = [
    'equal',
    'notEqual',
    'lessOrEqual',
    'greaterOrEqual',
    'range'
];

const NO_LIMIT = 1000000;

/**
 * Return true if two filters on same attribute can be used as single range filter.
 *
 * @param {Array.<Object>} filters
 * @return {boolean}
 * @private
 */
function filterRangeQueries(filters) {
    if (filters.length !== 2) return false;

    const rangeOperators = ['lessOrEqual', 'greaterOrEqual'];
    const operator1 = filters[0].operator;
    const operator2 = filters[1].operator;

    return rangeOperators.indexOf(operator1) !== -1 && rangeOperators.indexOf(operator2) !== -1;
}

/**
 * Create range filter from a greatOrEqual and lessOrEqual filter.
 *
 * @param {Array.<string>} attributeFilters
 * @return {Object}
 * @private
 */
function createRangeFilter(attributeFilters) {
    const rangeFilter = { attribute: attributeFilters[0].attribute, operator: 'range' };

    // make sure greaterOrEqual filter comes first
    attributeFilters.sort(filter1 => (filter1.operator === 'greaterOrEqual' ? -1 : 1));

    rangeFilter.value = [attributeFilters[0].value, attributeFilters[1].value];
    return rangeFilter;
}

/**
 * Convert greaterOrEqual and lessOrEqual filters on same attribute
 * to a single range filter.
 *
 * @param {Array.<Object>} filters
 * @return {Array.<Object>}
 * @private
 */
function rangify(filters) {
    const groupedAttrs = _.groupBy(filters, 'attribute');
    const rangeQueries = _.filter(groupedAttrs, filterRangeQueries);

    if (!rangeQueries.length) return filters;

    const rangeQueryAttrs = rangeQueries.map(rangeQuery => rangeQuery[0].attribute);

    // copy non-range query attributes
    return filters
        .filter(filter => rangeQueryAttrs.indexOf(filter.attribute) === -1)
        .concat(rangeQueries.map(createRangeFilter));
}

/**
 * @param {string} value
 * @return {string}
 * @private
 */
function escapeSpecialChars(value) {
    const specialCharRegex = /(\\|\/|\+|-|&|\||!|\(|\)|\{|}|\[|]|\^|"|~|\*|\?|:)/g;
    return value.replace(specialCharRegex, '\\$1');
}

/**
 * Escape strings and convert boolean values.
 *
 * @param {*} value
 * @return {*}
 * @private
 */
function escapeValueForSolr(value) {
    if (typeof value === 'string') value = escapeSpecialChars(value);
    else if (typeof value === 'boolean') value = value === false ? 0 : 1;
    return value;
}

/**
 * @param {Object} filter
 * @return {string}
 * @private
 */
function convertFilterToSolrSyntax(filter) {
    let value = filter.value;
    const operator = filter.operator;

    if (SUPPORTED_FILTERS.indexOf(filter.operator) === -1) {
        throw new ImplementationError(`DataSource "flora-solr" does not support "${filter.operator}" filters`);
    }

    if (!Array.isArray(filter.attribute)) {
        value = escapeValueForSolr(value);
        if (Array.isArray(value)) {
            if (operator === 'range') value = '[' + value[0] + ' TO ' + value[1] + ']';
            else value = '(' + value.join(' OR ') + ')';
        }

        if (operator !== 'notEqual') {
            if (operator === 'greaterOrEqual') value = '[' + value + ' TO *]';
            if (operator === 'lessOrEqual') value = '[* TO ' + value + ']';
            return filter.attribute + ':' + value;
        }
        return '-' + filter.attribute + ':' + value;
    }

    // convert composite keys to SOLR syntax
    return value
        .map((values) => {
            const conditions = values.map((val, index) => filter.attribute[index] + ':' + escapeValueForSolr(val));
            return '(' + conditions.join(' AND ') + ')';
        })
        .join(' OR ');
}

function buildSolrFilterString(floraFilters) {
    const orConditions = floraFilters.map((andFilters) => {
        if (andFilters.length > 1) andFilters = rangify(andFilters);
        const conditions = andFilters.map(convertFilterToSolrSyntax);
        return '(' + conditions.join(' AND ') + ')';
    });

    if (orConditions.length > 1) return '(' + orConditions.join(' OR ') + ')';

    return orConditions.join('');
}

/**
 * @param {Array.<Object>} floraOrders
 * @return {string}
 * @private
 */
function buildSolrOrderString(floraOrders) {
    return floraOrders
        .map(order => (order.attribute + ' ' + order.direction))
        .join(',');
}

function parseData(str) {
    try {
        return JSON.parse(str);
    } catch (e) {
        return new Error('Couldn\'t parse response: ' + str);
    }
}

function prepareQueryAddition(queryAdditions) {
    return queryAdditions
        .replace(/[\r\n]+/g, ' ')
        .replace(/\s{2,}/g, ' ')
        .trim();
}

/**
 *
 * @param {Object} requestUrl
 * @param {Object} params
 * @param {Function} callback
 * @private
 */
function querySolr(requestUrl, params, callback) {
    const options = url.parse(requestUrl);
    options.method = 'POST';
    options.headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    };

    const req = http.request(options, (res) => {
        const chunks = [];

        res.on('data', chunk => chunks.push(chunk));

        res.on('end', () => {
            const data = parseData(Buffer.concat(chunks).toString('utf8'));

            if (res.statusCode >= 400 || data instanceof Error) {
                const error = new Error('Solr error: ' + res.statusCode + ' ' + http.STATUS_CODES[res.statusCode]);
                if (data instanceof Error) {
                    error.message += ': ' + data.message;
                } else if (data && data.error && data.error.msg) {
                    error.message += ': ' + data.error.msg;
                }

                return callback(error);
            }

            return callback(null, { totalCount: data.response.numFound, data: data.response.docs });
        });
    });

    req.write(querystring.stringify(params)); // add params to POST body

    req.on('error', callback);
    req.end();
}

class DataSource {
    /**
     * @param {Api} api
     * @param {Object} config
     */
    constructor(api, config) {
        this.options = config;
    }

    /**
     * @public
     */
    prepare() {
    }

    /**
     * @param {Object} request
     * @param {Function} callback
     */
    process(request, callback) {
        const server = request.server || 'default';
        const queryParts = [];
        let params = { wt: 'json' };

        if (!this.options.servers[server]) return callback(new Error(`Server "${server}" not defined`));

        const requestUrl = this.options.servers[server].url + request.collection + '/select';

        if (request.attributes) params.fl = request.attributes.join(',');
        if (request.order) params.sort = buildSolrOrderString(request.order);

        if (request.search) queryParts.push(escapeValueForSolr(request.search));
        if (request.filter) {
            try {
                queryParts.push(buildSolrFilterString(request.filter));
            } catch (e) {
                return callback(e);
            }
        }
        if (request.queryAddition) queryParts.push(prepareQueryAddition(request.queryAddition));
        if (queryParts.length === 0) queryParts.push('*:*');

        // overwrite SOLR default limit for sub-resource processing
        if (!request.limit) request.limit = NO_LIMIT;
        if (request.page) params.start = (request.page - 1) * request.limit;

        if (!request.limitPer) params.rows = request.limit;
        else {
            params = _.assign(params, {
                group: 'true',
                'group.format': 'simple',
                'group.main': 'true',
                'group.field': request.limitPer,
                'group.limit': request.limit,
                rows: NO_LIMIT // disable default limit because groups are returned as list
            });
        }

        params.q = queryParts.join(' AND ');

        if (request._explain) {
            request._explain.url = requestUrl;
            request._explain.params = params;
        }

        return querySolr(requestUrl, params, callback);
    }

    /**
     * @param {Function} callback
     */
    close(callback) {
        // TODO: implement
        if (callback) callback();
    }

    /**
     * @param {...string} args
     */
    escape(...args) {
        return escapeValueForSolr(...args);
    }
}

module.exports = DataSource;
