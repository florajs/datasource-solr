'use strict';

var http = require('http'),
    url = require('url'),
    _ = require('lodash');

/**
 * @constructor
 * @param {Api} api
 * @param {Object} config
 */
var DataSource = module.exports = function (api, config) {
    this.options = config;

    // create config object for http.request method
    this.options.servers = _.mapValues(config.servers, function (server) {
        return _.assign(url.parse(server.url), { method: 'POST', headers: {} });
    });
};

/**
 * @public
 */
DataSource.prototype.prepare = function () {};

/**
 * @param {Object} request
 * @param {Function} callback
 */
DataSource.prototype.process = function (request, callback) {
    var options,
        requestOpts,
        server = request.server || 'default',
        params = ['wt=json'],
        queryString = '*:*',
        filters = [];

    if (!this.options.servers[server]) return callback(new Error('Server "' + server + '" not defined'));

    options = this.options.servers[server];
    requestOpts = _.assign({}, options, { path: options.pathname + request.collection + '/select' });

    if (request.attributes) params.push('fl=' + request.attributes.join(','));
    if (request.search) filters.push(escapeSpecialChars(request.search));
    if (request.filter) filters.push(buildSolrFilterString(request.filter));
    if (request.order) params.push('sort=' + encodeURIComponent(buildSolrOrderString(request.order)));

    if (filters.length) queryString = encodeURIComponent(filters.join(' AND '));

    if (!request.limit) request.limit = 1000000; // overwrite SOLR default limit for sub-resource processing
    if (request.page) params.push('start=' + (request.page - 1) * request.limit);
    params.push('rows=' + request.limit);

    params.push('q=' + queryString);

    if (request._explain) {
        request._explain.href = requestOpts.href;
        request._explain.params = params;
    }

    querySolr(requestOpts, params, callback);
};

/**
 * @param {Function} callback
 */
DataSource.prototype.close = function (callback) {
    // TODO: implement
    if (callback) callback();
};

function buildSolrFilterString(floraFilters) {
    var orConditions;

    orConditions = floraFilters.map(function (andFilters) {
        var conditions;
        if (andFilters.length > 1) andFilters = rangify(andFilters);
        conditions = andFilters.map(convertFilterToSolrSyntax);
        return '(' + conditions.join(' AND ') + ')';
    });

    return orConditions.join(' OR ');
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
    var groupedAttrs = _.groupBy(filters, 'attribute'),
        rangeQueries = _.filter(groupedAttrs, filterRangeQueries),
        rangeQueryAttrs;

    if (! rangeQueries.length) return filters;

    rangeQueryAttrs = rangeQueries.map(function (rangeQuery) {
        return rangeQuery[0].attribute;
    });

    return filters.filter(function (filter) { // copy non-range query attributes
            return rangeQueryAttrs.indexOf(filter.attribute) === -1;
        })
        .concat(rangeQueries.map(createRangeFilter));
}

/**
 * Return true if two filters on same attribute can be used as single range filter.
 *
 * @param {Array.<Object>} filters
 * @return {boolean}
 * @private
 */
function filterRangeQueries(filters) {
    var rangeOperators, operator1, operator2;

    if (filters.length !== 2) return false;

    rangeOperators = ['lessOrEqual', 'greaterOrEqual'];
    operator1 = filters[0].operator;
    operator2 = filters[1].operator;

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
    var rangeFilter = { attribute: attributeFilters[0].attribute, operator: 'range' };

    attributeFilters.sort(function (filter1) { // make sure greaterOrEqual filter comes first
        return filter1.operator === 'greaterOrEqual' ? -1 : 1;
    });

    rangeFilter.value = [attributeFilters[0].value, attributeFilters[1].value];
    return rangeFilter;
}

/**
 * @param {Object} filter
 * @return {string}
 * @private
 */
function convertFilterToSolrSyntax(filter) {
    var value = filter.value,
        operator = filter.operator;

    if (! Array.isArray(filter.attribute)) {
        value = prepareValueForSolr(value);
        if (Array.isArray(value)) {
            if (operator === 'range') value = '[' + value[0] + ' TO ' + value[1] + ']';
            else value = '(' + value.join(' OR ') + ')';
        }

        if (operator === 'greaterOrEqual') value = '[' + value + ' TO *]';
        if (operator === 'lessOrEqual') value = '[* TO ' + value + ']';

        return filter.attribute + ':' + value;
    } else { // convert composite keys to SOLR syntax
        return value
            .map(function (values) {
                var conditions = values.map(function (val, index) {
                    return filter.attribute[index] + ':' + prepareValueForSolr(val);
                });
                return '(' + conditions.join(' AND ') + ')';
            })
            .join(' OR ');
    }
}

/**
 * Escape strings and convert boolean values.
 *
 * @param {*} value
 * @return {*}
 * @private
 */
function prepareValueForSolr(value) {
    if (typeof value === 'string') value = escapeSpecialChars(value);
    else if (typeof value === 'boolean') value = value === false ? 0 : 1;
    return value;
}

/**
 *
 * @param {string} value
 * @return {string}
 * @private
 */
function escapeSpecialChars(value) {
    var specialCharRegex = /(\\|\/|\+|-|&|\||!|\(|\)|\{|}|\[|]|\^|"|~|\*|\?|:)/g;
    return value.replace(specialCharRegex, '\\$1');
}

/**
 * @param {Array.<Object>} floraOrders
 * @return {string}
 * @private
 */
function buildSolrOrderString(floraOrders) {
    var orderCriteria = floraOrders.map(function (order) {
        return order.attribute + ' ' + order.direction;
    });
    return orderCriteria.join(',');
}

/**
 *
 * @param {Object} options
 * @param {Array.<string>} params
 * @param {Function} callback
 * @private
 */
function querySolr(options, params, callback) {
    var req;

    options.headers['Content-Type'] = 'application/x-www-form-urlencoded';

    req = http.request(options, function processSolrReponse(res) {
        var str = '';

        res.on('data', function (chunk) {
            str += chunk;
        });

        res.on('end', function () {
            var data, error;

            if (res.statusCode >= 400) {
                error = new Error(http.STATUS_CODES[res.statusCode]);
                error.code = res.statusCode;
                return callback(error);
            }

            data = JSON.parse(str);
            callback(null, { totalCount: data.response.numFound, data: data.response.docs });
        });
    });

    req.on('error', callback);
    req.write(params.join('&'));
    req.end();
}
