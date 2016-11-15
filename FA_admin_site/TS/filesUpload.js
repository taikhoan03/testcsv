/// <reference path="jquery.d.ts" />
var file = (function () {
    function file() {
    }
    return file;
}());
function isNumber(x) {
    return typeof x === "number";
}
function showSelectedFile(files) {
    var $selectedFiles = $('#selectedFiles');
    var totalSize = 0;
    $selectedFiles.html('');
    for (var i = 0; i < files.length; i++) {
        var file = files[i];
        totalSize += file.size;
        var spanName = $("<span>").html(file.name).css('font-weight', 'bold');
        var infoItem = $("<div>").html("Name: ").append(spanName); //.html(+ ", size: " + file.size);
        $selectedFiles.append(infoItem);
    }
    $selectedFiles.append($('<div>').html('Total size: ' + formatSizeUnits(totalSize)).css({ 'font-weight': 'bold', 'color': 'green' }));
    //for (var i = 0; i < files.length; i++) {
    //    console.log(files[i]);
    //}
    //console.log($("body"));
}
function createEditionOption(divid) {
    var sel = $("<select id='edition'>");
    var limit = 100;
    for (var i = 1; i < limit; i++) {
        var op = $("<option>").val(i).html(i.toString());
        sel.append(op);
    }
    $("#" + divid).append(sel);
}
function createVersionOption(divid) {
    var sel = $("<select id='version'>");
    var limit = 100;
    for (var i = 1; i < limit; i++) {
        var op = $("<option>").val(i).html(i.toString());
        sel.append(op);
    }
    $("#" + divid).append(sel);
}
function formatSizeUnits(bytes) {
    var rs = '';
    if (bytes >= 1000000000) {
        rs = (bytes / 1000000000).toFixed(2) + ' GB';
    }
    else if (bytes >= 1000000) {
        rs = (bytes / 1000000).toFixed(2) + ' MB';
    }
    else if (bytes >= 1000) {
        rs = (bytes / 1000).toFixed(2) + ' KB';
    }
    else if (bytes > 1) {
        rs = bytes + ' bytes';
    }
    else if (bytes == 1) {
        rs = bytes + ' byte';
    }
    else {
        rs = '0 byte';
    }
    return rs;
}
//# sourceMappingURL=filesUpload.js.map