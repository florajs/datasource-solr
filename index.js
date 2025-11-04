'use strict';

const { ImplementationError } = require('@florajs/errors');

const SUPPORTED_FILTERS = ['equal', 'notEqual', 'less', 'lessOrEqual', 'greater', 'greaterOrEqual', 'range'];
const RANGE_OPERATOR_FILTER_MAPPING = {
    less: '}',
    lessOrEqual: ']',
    greater: '{',
    greaterOrEqual: '['
};

const NO_LIMIT = 1000000;

/**
 * Create range filter from a greatOrEqual and lessOrEqual filter.
 *
 * @param {Array.<string>} attributeFilters
 * @return {Object}
 * @private
 */
function createRangeFilter(attributeFilters) {
    // make sure greaterOrEqual filter comes first
    attributeFilters.sort((filter) => (['greater', 'greaterOrEqual'].includes(filter.operator) ? -1 : 1));

    return {
        attribute: attributeFilters[0].attribute,
        operator: 'range',
        lowerSolrRangeOperator: RANGE_OPERATOR_FILTER_MAPPING[attributeFilters[0].operator],
        upperSolrRangeOperator: RANGE_OPERATOR_FILTER_MAPPING[attributeFilters[1].operator],
        value: [attributeFilters[0].value, attributeFilters[1].value]
    };
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
    const hasRangeableFilters = filters.some((filter) =>
        ['less', 'lessOrEqual', 'greater', 'greaterOrEqual'].includes(filter.operator)
    );

    if (!hasRangeableFilters) {
        return filters;
    }

    const groupedAttrs = filters.reduce((acc, filter) => {
        acc[filter.attribute] = acc[filter.attribute] || [];
        acc[filter.attribute].push(filter);
        return acc;
    }, {});
    const rangeQueries = Object.values(groupedAttrs).filter((filters) => {
        if (filters.length !== 2) return false;

        const rangeOperators = ['less', 'lessOrEqual', 'greater', 'greaterOrEqual'];
        const operator1 = filters[0].operator;
        const operator2 = filters[1].operator;

        return (
            rangeOperators.includes(operator1) &&
            rangeOperators.includes(operator2) &&
            operator1.substring(0, 4) !== operator2.substring(0, 4)
        );
    });

    if (!rangeQueries.length) return filters;

    const rangeQueryAttrs = rangeQueries.map((rangeQuery) => rangeQuery[0].attribute);

    // copy non-range query attributes
    return filters
        .filter((filter) => !rangeQueryAttrs.includes(filter.attribute))
        .concat(rangeQueries.map(createRangeFilter));
}

/**
 * @param {string} value
 * @return {string}
 * @private
 */
function escapeSpecialChars(value) {
    const reservedKeywordRegex = /\b(AND|NOT|OR)\b/g;

    if (reservedKeywordRegex.test(value)) {
        value = value.replace(reservedKeywordRegex, (keyword) => keyword.toLowerCase());
    }

    return value.replace(/([\\/+\-&|!(){}[\]^"~*?:])/g, '\\$1');
}

/**
 * @param {string} value
 * @return {string}
 * @private
 */
function escapeSpecialCharsSolrSyntax(value) {
    return value.replace(/([\\/+\-&|!(){}[\]^~?])/g, '\\$1');
}

/**
 * Escape strings and convert boolean values.
 *
 * @param {*} value
 * @param {boolean} exposeSolrSyntax
 * @return {*}
 * @private
 */
function escapeValueForSolr(value, exposeSolrSyntax) {
    if (typeof value === 'string') {
        value = exposeSolrSyntax ? escapeSpecialCharsSolrSyntax(value) : escapeSpecialChars(value);
    } else if (typeof value === 'boolean') value = value === false ? 0 : 1;
    return value;
}

/**
 * @param {Object} filter
 * @return {string}
 * @private
 */
function convertFilterToSolrSyntax(filter) {
    const { operator } = filter;
    let { value } = filter;

    if (!SUPPORTED_FILTERS.includes(filter.operator)) {
        throw new ImplementationError(`DataSource "solr" does not support "${operator}" filters`);
    }

    if (!Array.isArray(filter.attribute)) {
        value = escapeValueForSolr(value);
        if (Array.isArray(value)) {
            const lowerSolrRangeOperator = filter.lowerSolrRangeOperator || '[';
            const upperSolrRangeOperator = filter.upperSolrRangeOperator || ']';

            value =
                operator === 'range'
                    ? lowerSolrRangeOperator + value[0] + ' TO ' + value[1] + upperSolrRangeOperator
                    : '(' + value.join(' OR ') + ')';
        }

        if (operator !== 'notEqual') {
            if (operator === 'greater') value = '{' + value + ' TO *]';
            if (operator === 'greaterOrEqual') value = '[' + value + ' TO *]';
            if (operator === 'less') value = '[* TO ' + value + '}';
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
    return floraOrders.map((order) => order.attribute + ' ' + order.direction).join(',');
}

function prepareQueryAddition(queryAdditions) {
    return queryAdditions
        .replace(/[\r\n]+/g, ' ')
        .replace(/\s{2,}/g, ' ')
        .trim();
}

/**
 * @param {Object} servers
 * @return {Object}
 */
function getUrlGenerators(servers) {
    return Object.keys(servers).reduce((acc, server) => {
        const { urls } = servers[server];

        acc[server] = (function* urlGenerator() {
            while (true) {
                for (let i = 0, l = urls.length; i < l; ++i) {
                    yield urls[i];
                }
            }
        })();

        return acc;
    }, {});
}

/**
 *
 * @param {string} requestUrl
 * @param {Object} params
 * @param {number} timeout
 * @returns {Promise}
 * @private
 */
async function querySolr(requestUrl, params, timeout) {
    const response = await fetch(requestUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams(params).toString(),
        signal: AbortSignal.timeout(timeout)
    });

    if (!response.ok) {
        throw new Error(`Solr error: ${response.status} - ${response.statusText}`);
    }

    const { numFound, docs } = (await response.json()).response;
    return { totalCount: numFound, data: docs };
}

function prepareSearchTerm(request) {
    const escapedSearchTerm = escapeValueForSolr(request.search, request.exposeSolrSyntax);
    const allowedSearchFields =
        request.allowedSearchFields && request.allowedSearchFields.trim()
            ? request.allowedSearchFields.trim().split(',')
            : [];

    if (!allowedSearchFields.length) {
        return escapedSearchTerm;
    }

    const regex = new RegExp(`^(?<field>${allowedSearchFields.join('|')}):(?<search>.+)`);
    const match = regex.exec(request.search);

    if (match === null) {
        return escapedSearchTerm;
    }

    return `(${match.groups.field}:"${match.groups.search}")`;
}

class DataSource {
    /**
     * @param {Api} api
     * @param {Object} config
     */
    constructor(api, config) {
        this.options = config;
        this._urls = getUrlGenerators(config.servers);
        this._status = config._status;
        delete config._status;
    }

    /**
     * @public
     */
    prepare() {}

    /**
     * @param {Object} request
     * @returns {Promise<Object>}
     */
    async process(request) {
        const server = request.server || 'default';
        const queryParts = [];
        const params = { wt: 'json' };
        const serverOpts = this.options.servers;

        if (!serverOpts[server]) throw new Error(`Server "${server}" not defined`);

        const requestUrl = this._urls[server].next().value + request.collection + '/select';

        if (request.attributes) params.fl = request.attributes.join(',');
        if (request.order) params.sort = buildSolrOrderString(request.order);

        if (request.df) params.df = request.df;
        if (request.search && request.search.trim()) queryParts.push(prepareSearchTerm(request));
        if (request.filter) {
            queryParts.push(buildSolrFilterString(request.filter));
        }
        if (request.queryAddition) queryParts.push(prepareQueryAddition(request.queryAddition));
        if (queryParts.length === 0) queryParts.push('*:*');

        // overwrite SOLR default limit for sub-resource processing
        if (!request.limit) request.limit = NO_LIMIT;
        if (request.page) params.start = (request.page - 1) * request.limit;

        if (!request.limitPer) params.rows = request.limit;
        else {
            Object.assign(params, {
                group: 'true',
                'group.format': 'simple',
                'group.main': 'true',
                'group.field': request.limitPer,
                'group.limit': request.limit,
                rows: NO_LIMIT // disable default limit because groups are returned as list
            });
        }

        params.q = queryParts.join(' AND ');

        if (request._explain) Object.assign(request._explain, { url: requestUrl, params });
        if (this._status) this._status.increment('dataSourceQueries');

        const timeout = serverOpts[server].timeout || 5000;
        return querySolr(requestUrl, params, timeout);
    }

    /**
     * @returns {Promise}
     */
    close() {
        return Promise.resolve();
    }

    /**
     * @param {...string} args
     */
    escape(...args) {
        return escapeValueForSolr(...args);
    }
}

module.exports = DataSource;
