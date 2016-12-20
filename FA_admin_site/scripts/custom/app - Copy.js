//require jquery
var prefix_id = 'dyn_';
function create_header_form(parent_id,num_of_col){
    var parent = $(parent_id);
    for (var i = 0; i < num_of_col; i++) {
        var label = $('<label>').html('Header ' + (i + 1) + ': ').attr('class','dyn_input_lbl');
        var input = $('<input>').attr({ 'type': 'text', 'id': prefix_id + i,'class':'dyn_input' });
        parent.append(label).append(input);
        //parent.append(input);
    }
}
function create_dyn_field_for_test(arr) {
    var parent = $(parent_id);
    for (var i = 0; i < arr.length; i++) {
        var label = $('<label>').html(arr[i]+ ': ').attr('class', 'dyn_input_lbl');
        var input = $('<input>').attr({ 'type': 'text', 'id': prefix_id + i, 'class': 'dyn_input' });
        parent.append(label).append(input);
        //parent.append(input);
    }
}
function jsonPropertyCount(obj) {
    var i = 0;
    for (var x in obj) {
        if (obj.hasOwnProperty(x)) {
            i++;
        }
    }
    return i;
}
function createTable(d, tableid) {
    var table = $("#" + tableid)
    var tr_header = table.find("#header");
    for (var i = 0; i < d.length; i++) {
        var th = $('<th>').html(d[i]);
        tr_header.append(th);
    }
    var tbody = table.find("#tbody");
    var tr = $('<tr>');
    
    for (var i = 0; i < d.length; i++) {
        if (d[i] == '_selected') {
            var td = $('<td>');//.html('{{' + key + '}}');
            var chk = $('<input>').attr({ 'type': 'checkbox' });
            td.append(chk);
            tr.append(td);
        } else {
            var td = $('<td>').html('{{'+d[i]+'}}');
            tr.append(td);
        }
        
    }
    tbody.append(tr);
}
function createTableFromJsonObj(d, tableid) {
    var table = $("#" + tableid);
    var p = d;
    var tr_header = table.find("#header");
    for (var key in p) {
        if (p.hasOwnProperty(key)) {
            var th = $('<th>').html(key);
            tr_header.append(th);
        }
    }
    var tbody = table.find("#tbody");
    var tr = $('<tr>');
    for (var key in p) {
        if (p.hasOwnProperty(key)) {
            if (key == '_selected') {
                var td = $('<td>');//.html('{{' + key + '}}');
                var chk = $('<input>').attr({ 'type': 'checkbox' });
                td.append(chk);
                tr.append(td);
            } else {
                var td = $('<td>').html('{{' + key + '}}');
                tr.append(td);
            }
            
        }
    }
    
    tbody.append(tr);
}
function createJsData(arr) {
    var header = arr[0];
    var rs = [];
    for (var j = 1; j < arr.length; j++) {
        var r = {};
        for (var i = 0; i < header.length; i++) {
            r[header[i]] = arr[j][i];
        }
        rs.push(r);
    }
    return rs;
}
///append _selected property to js array
function appendSelectedProperty(js_arr) {
    for (var i = 0; i < Object.keys(js_arr).length; i++) {
        js_arr[i]['_selected'] = false;
    }
    return js_arr;
}

function _createTableWithJsonObj(data, tableid, appendSelected, cb) {
    cb(data);
    $('#' + tableid + ' #tbody>tr').remove();
    $('#' + tableid + ' #header>th').remove();
    var header_arr = data[0];
    if (appendSelected)
        header_arr['_selected'] = false;

    var js_ = appendSelectedProperty(data);//createJsData(data).splice(0, 10);
    if (appendSelected)
        createTableFromJsonObj(header_arr, tableid);
    else
        createTable(header_arr, tableid);
    Tempo.prepare($('#' + tableid + ' #tbody>tr:first'), {'escape': false})
        .when(TempoEvent.Types.RENDER_COMPLETE, function (event) {
            $('#' + tableid + '').show();
            $('#' + tableid + ' #tbody>tr:last').remove();
        }).render(js_);
}
function _createTableWithJsonObj_withTemplate(data, tableid, appendSelected, cb_before,cb_after) {
    if (cb_before!=undefined)
        cb_before(data);
    //$('#' + tableid + ' #tbody>tr').remove();
    //$('#' + tableid + ' #header>th').remove();
    
    var js_ = appendSelectedProperty(data);
    $('#' + tableid + ' #tbody>tr:not(:first)').remove();
    Tempo.prepare($('#' + tableid + ' #tbody>tr:first'), { 'escape': false })
        .when(TempoEvent.Types.RENDER_COMPLETE, function (event) {
            $('#' + tableid + '').show();
            
            $('#' + tableid + ' #tbody>tr:first').hide();
        }).render(js_).clear();
    if (cb_after != undefined)
        cb_after(js_);
}
function _createTableWithArrObj(data, tableid, appendSelected,limit) {
    if (limit == undefined) limit = 10;
    $('#' + tableid + ' #tbody>tr').remove();
    $('#' + tableid + ' #header>th').remove();
    var header_arr = data[0];
    $('#' + tableid + '').show();

    var js_ = createJsData(data).splice(0, limit);
    createTable(header_arr, tableid);
    Tempo.prepare($('#' + tableid + ' #tbody>tr:first'), { 'escape': true })
        .when(TempoEvent.Types.RENDER_COMPLETE, function (event) {
            $('#' + tableid + '').show();
            $('#' + tableid + ' #tbody>tr:last').remove();
        }).render(js_).clear();
}

function isInt(value) {
    return !isNaN(value) &&
           parseInt(Number(value)) == value &&
           !isNaN(parseInt(value, 10));
}
var getUrlParameter = function getUrlParameter(sParam) {
    var sPageURL = decodeURIComponent(window.location.search.substring(1)),
        sURLVariables = sPageURL.split('&'),
        sParameterName,
        i;

    for (i = 0; i < sURLVariables.length; i++) {
        sParameterName = sURLVariables[i].split('=');

        if (sParameterName[0] === sParam) {
            return sParameterName[1] === undefined ? true : sParameterName[1];
        }
    }
};
// Rules
/*
    
*/
var default_limit = 10;//num of parameter limited
var at = {//rule accept_type
    number: 'number',
    string: 'string',
    bool: 'bool',
    char: 'char',
    any:'any'
}
var IRule = function (name, str_func, limit, delimiter) {
    this.name = name;
    this.F = str_func;//function string, Ex:AND(true,false) -> AND
    this.limitParam = limit;
    this.params = [];
    this.acceptType = [];
    this.delimiter = delimiter;
    this.addParam = function (param) {//param.value param.type
        if (this.params.length > limit) return;

        var nextRuleAcceptType = this.acceptType[this.params.length];
        if (nextRuleAcceptType == undefined) return;

        if(nextRuleAcceptType==at.any){
            this.params.push(param.value);
            return;
        }


        if (typeof nextRuleAcceptType == "string")
        {
            if (nextRuleAcceptType != param.type) {
                alert('Type is not valid');
                return;
            }
            //if char type
            if (nextRuleAcceptType == 'char') {
                param.value = param.value[0];
            }
        }
        else {//array
            var found = false;
            for (var i = 0; i < nextRuleAcceptType.length; i++) {
                
                if (nextRuleAcceptType[i] == param.type) {
                    found = true;
                    break;
                    
                }
                
            }
            if (!found) {
                alert('Type is not valid ' + nextRuleAcceptType);
                return;
            }
        }

        
        this.params.push(param.value);
    }
    this.result = function () {
        return str_func + '(' + this.params.join(this.delimiter) + ')';
    }
    this.showResult = function () {
        return this.result().replace(/\[\[\]\]/gi, ',');
    }
    this.setAcceptTypes = function (acceptTypesArr) {
        this.acceptType = acceptTypesArr;
    }
    this.setAllAcceptType = function (acceptType) {
        this.acceptType = [];
        for (var i = 0; i < default_limit; i++) {
            this.acceptType.push(acceptType);
        }
    }

};
var r_AND = new IRule("AND", "AND", default_limit, ',');
r_AND.setAcceptTypes([at.bool, at.bool, at.bool, at.bool, at.bool, at.bool, at.bool, at.bool, at.bool, at.bool]);
r_AND.showResult = function () {
    var str = "";
    for (var i = 0; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Value' + [i + 1] + ': </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: AND(Value1,Value2,...,Value' + default_limit + ') - this function will operate AND VALUE1 with other VALUE' + '<table>' + str + '</table>';
}
//r_AND.addParam({ value: true, type: at.bool });
//r_AND.addParam({ value: false, type: at.bool });
//r_AND.addParam({ value: 3, type: at.integer });
var r_CONTAINS = new IRule("CONTAINS", "CONTAINS", default_limit, '[[]]');
r_CONTAINS.setAllAcceptType(at.string);
r_CONTAINS.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    for (var i = 1; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Contain : </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: Contains(Target,Value1,Value2...) - this function will check if TARGET is contain VALUE1 and VALUE2 and ...' + '<table>' + str + '</table>';
}

var r_EQUAL = new IRule("EQUAL", "EQUAL", default_limit, ',');
r_EQUAL.setAcceptTypes([at.any, at.any]);
//r_EQUAL.setAcceptTypes([[at.string, at.number], [at.string, at.bool]]);
var r_GREATER_THAN = new IRule("GREATER_THAN", "GREATER_THAN", default_limit, ',');
r_GREATER_THAN.setAcceptTypes([at.number, at.number]);


var r_GREATER_THAN_OR_EQUAL = new IRule("GREATER_THAN_OR_EQUAL", "GREATER_THAN_OR_EQUAL", default_limit, ',');
r_GREATER_THAN_OR_EQUAL.setAcceptTypes([at.number, at.number]);

var r_IS_ALPHA = new IRule("IS_ALPHA", "IS_ALPHA", default_limit, ',');
r_IS_ALPHA.setAcceptTypes([at.string]);

var r_IS_NULL = new IRule("IS_NULL", "IS_NULL", default_limit, ',');
r_IS_NULL.setAcceptTypes([at.string]);

var r_IS_NUMERIC = new IRule("IS_NUMERIC", "IS_NUMERIC", default_limit, ',');
r_IS_NUMERIC.setAcceptTypes([at.string,at.number]);

var r_LESS_THAN = new IRule("LESS_THAN", "LESS_THAN", default_limit, ',');
r_LESS_THAN.setAcceptTypes([at.number, at.number]);

var r_LESS_THAN_OR_EQUAL = new IRule("LESS_THAN_OR_EQUAL", "LESS_THAN_OR_EQUAL", default_limit, ',');
r_LESS_THAN_OR_EQUAL.setAcceptTypes([at.number, at.number]);

var r_NOT = new IRule("NOT", "NOT", default_limit, ',');
r_NOT.setAcceptTypes([at.bool]);

var r_NOT_CONTAINS = new IRule("NOT_CONTAINS", "NOT_CONTAINS", default_limit, '[[]]');
r_NOT_CONTAINS.setAllAcceptType([at.string]);

var r_NOT_EQUAL = new IRule("NOT_EQUAL", "NOT_EQUAL", default_limit, ',');
r_NOT_EQUAL.setAcceptTypes([[at.string, at.number], [at.string, at.number]]);

var r_OR = new IRule("OR", "OR", default_limit, ',');
r_OR.setAllAcceptType(at.bool);

var r_RANGE = new IRule("RANGE", "RANGE", default_limit, ',');
r_RANGE.setAcceptTypes([at.number, at.number, at.number]);

var r_STARTS_WITH = new IRule("STARTS_WITH", "STARTS_WITH", default_limit, '[[]]');
r_STARTS_WITH.setAllAcceptType(at.any);



/*


*/
var r_CONCATENATE = new IRule("CONCATENATE", "CONCATENATE", default_limit, '[[]]');
r_CONCATENATE.setAllAcceptType(at.string);

var r_JOIN = new IRule("JOIN", "JOIN", default_limit, '[[]]');
r_JOIN.setAllAcceptType(at.string);

var r_LEFT_PAD = new IRule("LEFT_PAD", "LEFT_PAD", default_limit, '[[]]');
r_LEFT_PAD.setAcceptTypes([at.string,at.string,at.number]);

var r_LEFT_TRIM = new IRule("LEFT_TRIM", "LEFT_TRIM", default_limit, '[[]]');
r_LEFT_TRIM.setAcceptTypes([at.string, at.string]);

var r_REMOVE_CHARACTERS = new IRule("REMOVE_CHARACTERS", "REMOVE_CHARACTERS", default_limit, '[[]]');
r_REMOVE_CHARACTERS.setAllAcceptType(at.string);

var r_REPLACE_CONTAINS = new IRule("REPLACE_CONTAINS", "REPLACE_CONTAINS", default_limit, '[[]]');
r_REPLACE_CONTAINS.setAcceptTypes([at.string,at.string,at.string]);

var r_RIGHT_PAD = new IRule("RIGHT_PAD", "RIGHT_PAD", default_limit, '[[]]');
r_RIGHT_PAD.setAcceptTypes([at.string, at.string, at.number]);

var r_RIGHT_TRIM = new IRule("RIGHT_TRIM", "RIGHT_TRIM", default_limit, '[[]]');
r_RIGHT_TRIM.setAcceptTypes([at.string, at.string]);

var r_SPLIT = new IRule("SPLIT", "SPLIT", default_limit, '[[]]');
r_SPLIT.setAcceptTypes([at.string, at.string, at.number]);

var r_SUB_STRING = new IRule("SUB_STRING", "SUB_STRING", default_limit, '[[]]');
r_SUB_STRING.setAcceptTypes([at.string, at.number, at.number]);

var r_UPPER = new IRule("UPPER", "UPPER", default_limit, '[[]]');
r_UPPER.setAcceptTypes([at.string]);



var r_AS_IS = new IRule("AS_IS", "AS_IS", default_limit, '');
r_AS_IS.setAcceptTypes([[at.string, at.number]]);

var r_LAST_POSITION_OF = new IRule("LAST_POSITION_OF", "LAST_POSITION_OF", default_limit, '[[]]');
r_LAST_POSITION_OF.setAcceptTypes([at.string, at.string]);

var r_POSITION_OF = new IRule("POSITION_OF", "POSITION_OF", default_limit, '[[]]');
r_POSITION_OF.setAcceptTypes([at.string, at.string]);

var r_LENGTH = new IRule("LENGTH", "LENGTH", default_limit, '');
r_LENGTH.setAcceptTypes([at.string]);

var r_MAX = new IRule("MAX", "MAX", default_limit, ',');
r_MAX.setAllAcceptType(at.number);

var r_MIN = new IRule("MIN", "MIN", default_limit, ',');
r_MIN.setAllAcceptType(at.number);

var r_ROUND = new IRule("ROUND", "ROUND", default_limit, ',');
r_ROUND.setAcceptTypes([at.number, at.number]);

var r_STRING_COUNT = new IRule("STRING_COUNT", "STRING_COUNT", default_limit, ',');
r_STRING_COUNT.setAcceptTypes([at.string, at.string]);

var r_SUM_ALL = new IRule("SUM_ALL", "SUM_ALL", default_limit, ',');
r_SUM_ALL.setAllAcceptType(at.number);

var currentRule = {};
var app_rules = [r_AND,
    r_CONTAINS,
    r_EQUAL,
r_GREATER_THAN,
r_GREATER_THAN_OR_EQUAL,
r_IS_ALPHA,
r_IS_NULL,
r_IS_NUMERIC,
r_LESS_THAN,
r_LESS_THAN_OR_EQUAL,
r_NOT,
r_NOT_CONTAINS,
r_NOT_EQUAL,
r_OR,
r_RANGE,
r_STARTS_WITH,
//formatting
r_CONCATENATE,
r_JOIN,
r_LEFT_PAD,
r_LEFT_TRIM,
r_REMOVE_CHARACTERS,
r_REPLACE_CONTAINS,
r_RIGHT_PAD,
r_RIGHT_TRIM,
r_SPLIT,
r_SUB_STRING,
//MISCELLANEOUS
r_AS_IS,
r_LAST_POSITION_OF,
r_POSITION_OF,
r_LENGTH,
r_MAX,
r_MIN,
r_ROUND,
r_STRING_COUNT,
r_SUM_ALL
];
function addRule() {
    var rule = new IRule();
}