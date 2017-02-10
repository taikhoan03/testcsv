//require jquery
var prefix_id = 'dyn_';
function create_header_form(parent_id, num_of_col) {
    var parent = $(parent_id);
    for (var i = 0; i < num_of_col; i++) {
        var label = $('<label>').html('Header ' + (i + 1) + ': ').attr('class', 'dyn_input_lbl');
        var input = $('<input>').attr({ 'type': 'text', 'id': prefix_id + i, 'class': 'dyn_input' });
        parent.append(label).append(input);
        //parent.append(input);
    }
}
function create_dyn_field_for_test(arr) {
    var parent = $(parent_id);
    for (var i = 0; i < arr.length; i++) {
        var label = $('<label>').html(arr[i] + ': ').attr('class', 'dyn_input_lbl');
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
            var td = $('<td>').html('{{' + d[i] + '}}');
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

function _createTableWithJsonObj(data, tableid, appendSelected, cb,cb_after) {
    if (cb!=undefined)
        cb(data);
    $('#' + tableid + ' #tbody>tr').remove();
    $('#' + tableid + ' #header>th').remove();
    var header_arr = data[0];
    var js_ = data;
    
    if (appendSelected) {
        header_arr['_selected'] = false;
        js_ = appendSelectedProperty(data);
    }
    

    //var js_ = appendSelectedProperty(data);//createJsData(data).splice(0, 10);
    //if (appendSelected)
        createTableFromJsonObj(header_arr, tableid);
    //else
    //    createTable(header_arr, tableid);
    Tempo.prepare($('#' + tableid + ' #tbody>tr:first'), { 'escape': false })
        .when(TempoEvent.Types.RENDER_COMPLETE, function (event) {
            $('#' + tableid + '').show();
            $('#' + tableid + ' #tbody>tr:last').remove();
            if (cb_after != undefined)
                cb_after(data);
        }).render(js_);
}
function _createTableWithJsonObj_withTemplate(data, tableid, appendSelected, cb_before, cb_after) {
    if (cb_before != undefined)
        cb_before(data);
    //$('#' + tableid + ' #tbody>tr').remove();
    //$('#' + tableid + ' #header>th').remove();

    var js_ = appendSelectedProperty(data);
    $('#' + tableid + ' #tbody>tr:not(:first)').remove();
    Tempo.prepare($('#' + tableid + ' #tbody>tr:first'), { 'escape': false })
        .when(TempoEvent.Types.RENDER_COMPLETE, function (event) {
            $('#' + tableid + '').show();

            $('#' + tableid + ' #tbody>tr:first').hide();
            if (cb_after != undefined)
                cb_after(js_);
        }).render(js_).clear();
    
}
function _createTableWithArrObj(data, tableid, appendSelected, limit) {
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
    any: 'any'
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

        if (nextRuleAcceptType == at.any) {
            this.params.push(param.value);
            return;
        }


        if (typeof nextRuleAcceptType == "string") {
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
        if (i > 1)
            $lbl = '<tr><th class="lbl-arg"><b>AND</b> Contain : </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: Contains(Target,Value1,Value2...) - this function will check if TARGET is contain VALUE1 and VALUE2 and ...' + '<table>' + str + '</table>';
}

var r_EQUAL = new IRule("EQUAL", "EQUAL", default_limit, ',');
r_EQUAL.setAcceptTypes([at.any, at.any]);
//r_EQUAL.setAcceptTypes([[at.string, at.number], [at.string, at.bool]]);
r_EQUAL.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Value1 : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Value2 : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: EQUAL(Value1,Value2) - this function will check if VALUE1=VALUE2' + '<table>' + str + '</table>';
}

var r_GREATER_THAN = new IRule("GREATER_THAN", "GREATER_THAN", default_limit, ',');
r_GREATER_THAN.setAcceptTypes([at.number, at.number]);
r_GREATER_THAN.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Value1 : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Value2 : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: GREATER_THAN(Value1,Value2) - this function will check if VALUE1>VALUE2' + '<table>' + str + '</table>';
}

var r_GREATER_THAN_OR_EQUAL = new IRule("GREATER_THAN_OR_EQUAL", "GREATER_THAN_OR_EQUAL", default_limit, ',');
r_GREATER_THAN_OR_EQUAL.setAcceptTypes([at.number, at.number]);
r_GREATER_THAN_OR_EQUAL.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Value1 : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Value2 : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: GREATER_THAN_OR_EQUAL(Value1,Value2) - this function will check if VALUE1>=VALUE2' + '<table>' + str + '</table>';
}

var r_IS_ALPHA = new IRule("IS_ALPHA", "IS_ALPHA", default_limit, ',');
r_IS_ALPHA.setAcceptTypes([at.string]);
r_IS_ALPHA.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Value : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: IS_ALPHA(Value) - this function will check if VALUE is ALPHA' + '<table>' + str + '</table>';
}

var r_IS_NULL = new IRule("IS_NULL", "IS_NULL", default_limit, ',');
r_IS_NULL.setAcceptTypes([at.string]);
r_IS_NULL.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Value : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: IS_NULL(Value) - this function will check if VALUE is NULL' + '<table>' + str + '</table>';
}

var r_IS_NUMERIC = new IRule("IS_NUMERIC", "IS_NUMERIC", default_limit, ',');
r_IS_NUMERIC.setAcceptTypes([at.string, at.number]);
r_IS_NUMERIC.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Value : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: IS_NUMERIC(Value) - this function will check if VALUE is NUMERIC' + '<table>' + str + '</table>';
}

var r_LESS_THAN = new IRule("LESS_THAN", "LESS_THAN", default_limit, ',');
r_LESS_THAN.setAcceptTypes([at.number, at.number]);
r_LESS_THAN.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Value1 : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Value2 : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: LESS_THAN(Value1,Value2) - this function will check if VALUE1 < VALUE2' + '<table>' + str + '</table>';
}

var r_LESS_THAN_OR_EQUAL = new IRule("LESS_THAN_OR_EQUAL", "LESS_THAN_OR_EQUAL", default_limit, ',');
r_LESS_THAN_OR_EQUAL.setAcceptTypes([at.number, at.number]);
r_LESS_THAN_OR_EQUAL.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Value1 : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Value2 : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: LESS_THAN_OR_EQUAL(Value1,Value2) - this function will check if VALUE1<=VALUE2' + '<table>' + str + '</table>';
}

var r_NOT = new IRule("NOT", "NOT", default_limit, ',');
r_NOT.setAcceptTypes([at.bool]);
r_NOT.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Value : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: NOT(Value) - this function will operate NOT the VALUE' + '<table>' + str + '</table>';
}

var r_NOT_CONTAINS = new IRule("NOT_CONTAINS", "NOT_CONTAINS", default_limit, '[[]]');
r_NOT_CONTAINS.setAllAcceptType([at.string]);
r_NOT_CONTAINS.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    for (var i = 1; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Not Inside : </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: NOT_CONTAINS(Target,Value1,Value2...) - this function will check if TARGET is not contain VALUE1 and VALUE2 and ...' + '<table>' + str + '</table>';
}

var r_NOT_EQUAL = new IRule("NOT_EQUAL", "NOT_EQUAL", default_limit, ',');
r_NOT_EQUAL.setAcceptTypes([[at.string, at.number], [at.string, at.number]]);
r_NOT_EQUAL.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Value 1 : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Value 2 : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: NOT_EQUAL(Value1,Value2) - this function will check if VALUE1 <> VALUE2' + '<table>' + str + '</table>';
}

var r_OR = new IRule("OR", "OR", default_limit, ',');
r_OR.setAllAcceptType(at.bool);
r_OR.showResult = function () {
    var str = "";
    for (var i = 0; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Value ' + [i + 1] + ': </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: OR(Value1,Value2,...,Value' + default_limit + ') - this function will operate OR VALUE1 with other VALUE' + '<table>' + str + '</table>';
}

var r_RANGE = new IRule("RANGE", "RANGE", default_limit, ',');
r_RANGE.setAcceptTypes([at.number, at.number, at.number]);
r_RANGE.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    var r3 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    if (this.params.length >= 3) {
        r3 = this.params[2].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Value : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">From : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">To : </th>';
    $arg = '<td class="arg"> ' + r3 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: RANGE(Value,From,To) - this function will check if VALUE is between FROM and TO' + '<table>' + str + '</table>';
}

var r_STARTS_WITH = new IRule("STARTS_WITH", "STARTS_WITH", default_limit, '[[]]');
r_STARTS_WITH.setAllAcceptType(at.any);
r_STARTS_WITH.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    for (var i = 1; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Start with : </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: STARTS_WITH(Target,Value1,Value2...) - this function will check if TARGET is Start with VALUE1 and VALUE2 and ...' + '<table>' + str + '</table>';
}


/*


*/
var r_CONCATENATE = new IRule("CONCATENATE", "CONCATENATE", default_limit, '[[]]');
r_CONCATENATE.setAllAcceptType(at.string);
r_CONCATENATE.showResult = function () {
    var str = "";
    for (var i = 0; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Value' + [i + 1] + ' : </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: CONCATENATE(Value1,Value2,...) - this function will return a string from combination of VALUE1, VALUE2, ...' + '<table>' + str + '</table>';
}

var r_JOIN = new IRule("JOIN", "JOIN", default_limit, '[[]]');
r_JOIN.setAllAcceptType(at.string);
r_JOIN.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Seperator : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    for (var i = 1; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Value' + [i] + ' : </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: JOIN(Seperator,Value1,Value2,...) - this function will return a string from combination of VALUE1, VALUE2, ... and seperate by SEPERATOR' + '<table>' + str + '</table>';
}

var r_LEFT_PAD = new IRule("LEFT_PAD", "LEFT_PAD", default_limit, '[[]]');
r_LEFT_PAD.setAcceptTypes([at.string, at.string, at.number]);
r_LEFT_PAD.showResult = function () {
    var str = "";
    var lp1 = "";
    var lp2 = "";
    var lp3 = "";
    if (this.params.length >= 1) {
        lp1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        lp2 = this.params[1].replace(/{|}/g, "");
    }
    if (this.params.length >= 3) {
        lp3 = this.params[2].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + lp1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">String : </th>';
    $arg = '<td class="arg"> ' + lp2 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Length : </th>';
    $arg = '<td class="arg"> ' + lp3 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: LEFT_PAD(Target,String,Length) - this function will add STRING to left of TARGET until the result have enough LENGTH' + '<table>' + str + '</table>';
}

var r_LEFT_TRIM = new IRule("LEFT_TRIM", "LEFT_TRIM", default_limit, '[[]]');
r_LEFT_TRIM.setAcceptTypes([at.string, at.string]);
r_LEFT_TRIM.showResult = function () {
    var str = "";
    var l1 = "";
    var l2 = "";
    var l3 = "";
    if (this.params.length >= 1) {
        lp1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        lp2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + l1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">String : </th>';
    $arg = '<td class="arg"> ' + l2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: LEFT_TRIM(Target,String) - this function will remove exact STRING from left of TARGET' + '<table>' + str + '</table>';
}

var r_REMOVE_CHARACTERS = new IRule("REMOVE_CHARACTERS", "REMOVE_CHARACTERS", default_limit, '[[]]');
r_REMOVE_CHARACTERS.setAllAcceptType(at.string);
r_REMOVE_CHARACTERS.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    for (var i = 1; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Value' + [i] + ' : </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: REMOVE_CHARACTERS(Target,Value1,Value2,...) - this function will remove all VALUE in TARGET' + '<table>' + str + '</table>';
}

var r_REPLACE_STRING = new IRule("REPLACE_STRING", "REPLACE_STRING", default_limit, '[[]]');
r_REPLACE_STRING.setAcceptTypes([at.string, at.string, at.string]);
r_REPLACE_STRING.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    var r3 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    if (this.params.length >= 3) {
        r3 = this.params[2].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Find : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">New : </th>';
    $arg = '<td class="arg"> ' + r3 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: REPLACE_STRING(Target,Find,New) - this function will replace FIND with NEW in TARGET' + '<table>' + str + '</table>';
}

var r_REPLACE_CONTAINS = new IRule("REPLACE_CONTAINS", "REPLACE_CONTAINS", default_limit, '[[]]');
r_REPLACE_CONTAINS.setAllAcceptType(at.string);
r_REPLACE_CONTAINS.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    var r3 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    
    var $lbl = '<tr><th class="lbl-arg">Replace to : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Source : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    
    for (var i = 2; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">String (find) : </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: REPLACE_CONTAINS(D1, S, D2, D3,...) - Dùng để thay thế nhiều chuỗi/kí tự nào đó trong 1 nguồn thành 1 kết quả duy nhất. Trong đó D1 là kết quả thay thế còn D2, D3 là dữ liệu bị thay thế có sẵn trong nguồn     Ex: REPLACE CONTAINS (PO BOX, Source, BOX, B0X, P.B., POBOX) = PO BOX' + '<table>' + str + '</table>';
}

var r_RIGHT_PAD = new IRule("RIGHT_PAD", "RIGHT_PAD", default_limit, '[[]]');
r_RIGHT_PAD.setAcceptTypes([at.string, at.string, at.number]);
r_RIGHT_PAD.showResult = function () {
    var str = "";
    var rp1 = "";
    var rp2 = "";
    var rp3 = "";
    if (this.params.length >= 1) {
        rp1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        rp2 = this.params[1].replace(/{|}/g, "");
    }
    if (this.params.length >= 3) {
        rp3 = this.params[2].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + rp1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">String : </th>';
    $arg = '<td class="arg"> ' + rp2 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Length : </th>';
    $arg = '<td class="arg"> ' + rp3 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: RIGHT_PAD(Target,String,Length) - this function will add STRING to right of TARGET until the result have enough LENGTH' + '<table>' + str + '</table>';
}

var r_RIGHT_TRIM = new IRule("RIGHT_TRIM", "RIGHT_TRIM", default_limit, '[[]]');
r_RIGHT_TRIM.setAcceptTypes([at.string, at.string]);
r_RIGHT_TRIM.showResult = function () {
    var str = "";
    var r1 = "";
    var r2 = "";
    if (this.params.length >= 1) {
        r1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        r2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + r1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">String : </th>';
    $arg = '<td class="arg"> ' + r2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: RIGHT_TRIM(Target,String) - this function will remove exact STRING from right of TARGET' + '<table>' + str + '</table>';
}

var r_SPLIT = new IRule("SPLIT", "SPLIT", default_limit, '[[]]');
r_SPLIT.setAcceptTypes([at.string, at.string, at.number]);
r_SPLIT.showResult = function () {
    var str = "";
    var s1 = "";
    var s2 = "";
    var s3 = "";
    if (this.params.length >= 1) {
        s1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        s2 = this.params[1].replace(/{|}/g, "");
    }
    if (this.params.length >= 3) {
        s3 = this.params[2].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + s1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Separate : </th>';
    $arg = '<td class="arg"> ' + s2 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Order : </th>';
    $arg = '<td class="arg"> ' + s3 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: SPLIT(Target,Separate,Length) - this function will' + '<table>' + str + '</table>';
}

var r_SUB_STRING = new IRule("SUB_STRING", "SUB_STRING", default_limit, '[[]]');
r_SUB_STRING.setAcceptTypes([at.string, at.number, at.number]);
r_SUB_STRING.showResult = function () {
    var str = "";
    var s1 = "";
    var s2 = "";
    var s3 = "";
    if (this.params.length >= 1) {
        s1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        s2 = this.params[1].replace(/{|}/g, "");
    }
    if (this.params.length >= 3) {
        s3 = this.params[2].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + s1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Start : </th>';
    $arg = '<td class="arg"> ' + s2 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Length : </th>';
    $arg = '<td class="arg"> ' + s3 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: SUB_STRING(Target,Start,Length) - this function will' + '<table>' + str + '</table>';
}

var r_UPPER = new IRule("UPPER", "UPPER", default_limit, '[[]]');
r_UPPER.setAcceptTypes([at.string]);
r_UPPER.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: UPPER(Target) - this function will convert TARGET to upper case' + '<table>' + str + '</table>';
}



var r_AS_IS = new IRule("AS_IS", "AS_IS", default_limit, '');
r_AS_IS.setAcceptTypes([at.any]);
r_AS_IS.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Value : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: AS_IS(Value) - this function will return VALUE' + '<table>' + str + '</table>';
}

var r_LAST_POSITION_OF = new IRule("LAST_POSITION_OF", "LAST_POSITION_OF", default_limit, '[[]]');
r_LAST_POSITION_OF.setAcceptTypes([at.string, at.string]);
r_LAST_POSITION_OF.showResult = function () {
    var str = "";
    var s1 = "";
    var s2 = "";
    if (this.params.length >= 1) {
        s1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        s2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + s1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Find : </th>';
    $arg = '<td class="arg"> ' + s2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: LAST_POSITION_OF(Target,Find) - this function will return the last position of FIND in TARGET' + '<table>' + str + '</table>';
}

var r_POSITION_OF = new IRule("POSITION_OF", "POSITION_OF", default_limit, '[[]]');
r_POSITION_OF.setAcceptTypes([at.string, at.string]);
r_POSITION_OF.showResult = function () {
    var str = "";
    var s1 = "";
    var s2 = "";
    if (this.params.length >= 1) {
        s1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        s2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + s1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Find : </th>';
    $arg = '<td class="arg"> ' + s2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: POSITION_OF(Target,Find) - this function will return the first position of FIND in TARGET' + '<table>' + str + '</table>';
}

var r_LENGTH = new IRule("LENGTH", "LENGTH", default_limit, '');
r_LENGTH.setAcceptTypes([at.string]);
r_LENGTH.showResult = function () {
    var str = "";
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var f = "";
    if (this.params[0])
        f = this.params[0].replace(/{|}/g, "");
    var $arg = '<td class="arg">' + f + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: LENGTH(Target) - this function will return the LENGTH of TARGET' + '<table>' + str + '</table>';
}

var r_MAX = new IRule("MAX", "MAX", default_limit, ',');
r_MAX.setAllAcceptType(at.number);
r_MAX.showResult = function () {
    var str = "";
    for (var i = 0; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Value' + [i + 1] + ': </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: MAX(Value1,Value2,...,Value' + default_limit + ') - this function will return the Maximum of Values' + '<table>' + str + '</table>';
}

var r_MIN = new IRule("MIN", "MIN", default_limit, ',');
r_MIN.setAllAcceptType(at.number);
r_MIN.showResult = function () {
    var str = "";
    for (var i = 0; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Value' + [i + 1] + ': </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: MIN(Value1,Value2,...,Value' + default_limit + ') - this function will return the Minimum of Values' + '<table>' + str + '</table>';
}

var r_ROUND = new IRule("ROUND", "ROUND", default_limit, ',');
r_ROUND.setAcceptTypes([at.number, at.number]);
r_ROUND.showResult = function () {
    var str = "";
    var s1 = "";
    var s2 = "";
    if (this.params.length >= 1) {
        s1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        s2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + s1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Digit : </th>';
    $arg = '<td class="arg"> ' + s2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: ROUND(Target,Digit) - this function will round TARGET to number of DIGIT  ' + '<table>' + str + '</table>';
}

var r_STRING_COUNT = new IRule("STRING_COUNT", "STRING_COUNT", default_limit, ',');
r_STRING_COUNT.setAcceptTypes([at.string, at.string]);
r_STRING_COUNT.showResult = function () {
    var str = "";
    var s1 = "";
    var s2 = "";
    if (this.params.length >= 1) {
        s1 = this.params[0].replace(/{|}/g, "");
    }
    if (this.params.length >= 2) {
        s2 = this.params[1].replace(/{|}/g, "");
    }
    var $lbl = '<tr><th class="lbl-arg">Target : </th>';
    var $arg = '<td class="arg"> ' + s1 + '</td></tr>';
    str += $lbl + $arg;
    $lbl = '<tr><th class="lbl-arg">Find : </th>';
    $arg = '<td class="arg"> ' + s2 + '</td></tr>';
    str += $lbl + $arg;
    return 'Example: STRING_COUNT(Target,FIND) - this function will count how many FIND in TARGET' + '<table>' + str + '</table>';
}

var r_SUM_ALL = new IRule("SUM_ALL", "SUM_ALL", default_limit, ',');
r_SUM_ALL.setAllAcceptType(at.number);
r_SUM_ALL.showResult = function () {
    var str = "";
    for (var i = 0; i < this.params.length; i++) {
        var $lbl = '<tr><th class="lbl-arg">Value' + [i + 1] + ': </th>';
        var $arg = '<td class="arg">' + this.params[i].replace(/{|}/g, "") + '</td></tr>';
        str += $lbl + $arg;
    }
    return 'Example: SUM_ALL(Value1,Value2,...,Value' + default_limit + ') - this function will operate SUM all VALUE' + '<table>' + str + '</table>';
}


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
r_REPLACE_STRING,
r_RIGHT_PAD,
r_RIGHT_TRIM,
r_SPLIT,
r_SUB_STRING,
r_UPPER,
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
//
function convertToClientType(idType) {
    if (idType == 1 || idType == 4) {
        return "TEXT";
    } else if (idType == 0 || idType == 3) {
        return "NUM";
    }
    else if (idType == 2) {
        return "BOOL";
    }
}
//begin rule math
function showItemForMATH() {
    var items = $('#r_rules .f_item');
    for (var i = 0; i < items.length; i++) {
        var item = $(items[i]);
        if (!item.hasClass('number'))
            item.hide();
        else
            item.show();
    }
    items = $('#r_fields .f_item');
    for (var i = 0; i < items.length; i++) {
        var item = $(items[i]);
        if (!item.hasClass('number'))
            item.hide();
        else
            item.show();
    }
}
function clean_cbox_selection() {
    $('#cboxCondition').val('').trigger('change');
    $('#cboxFormatting').val('').trigger('change');
    $('#cboxMISCELLANEOUS').val('').trigger('change');
}
function format_transform_status(jsArr,arrRemove) {
    for (var i = 0; i < jsArr.length; i++) {
        if (jsArr[i].Status == 0) {
            jsArr[i].Status_name = 'Waiting';
        } else if (jsArr[i].Status == 1) {
            jsArr[i].Status_name = 'Processing';
        }
        else if (jsArr[i].Status == 2) {
            jsArr[i].Status_name = 'Done';
        }
        else if (jsArr[i].Status == 3) {
            jsArr[i].Status_name = 'FAIL';
        }
        else if (jsArr[i].Status == 4) {
            jsArr[i].Status_name = 'Detached';
        }
    }
    for (var i = 0; i < jsArr.length; i++) {
        var item = jsArr[i];
        for (var j = 0; j < arrRemove.length; j++) {
            delete item[arrRemove[j]];
        }
    }
    
}
//end rule math