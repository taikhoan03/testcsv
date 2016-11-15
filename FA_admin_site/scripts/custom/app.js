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
function _createTableWithJsonObj_withTemplate(data, tableid, appendSelected, cb_before) {
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
}
function _createTableWithArrObj(data, tableid, appendSelected) {
    
    //$('#' + tableid + ' #tbody>tr').remove();
    //$('#' + tableid + ' #header>th').remove();
    var header_arr = data[0];
    $('#' + tableid + '').show();

    var js_ = createJsData(data).splice(0, 10);
    createTable(header_arr, tableid);
    Tempo.prepare($('#' + tableid + ' #tbody>tr:first'), { 'escape': true })
        .when(TempoEvent.Types.RENDER_COMPLETE, function (event) {
            $('#' + tableid + '').show();
            $('#' + tableid + ' #tbody>tr:last').remove();
        }).render(js_).clear();
}