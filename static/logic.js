function cmd(){
    var valueAB = document.getElementById("selectAB").value;
    var date_from = document.getElementById("date_from").value;
    var date_before = document.getElementById("date_before").value;

    $.ajax({
        type: "POST",
        url: "http://10.1.241.41:9300/cmd",
        data: {"idAB": valueAB, "date_from": date_from, "date_before":date_before},  
        success: function (data) { 
            var returnedData = JSON.parse(data);

            
            var wb = XLSX.utils.book_new();
            wb.Props = {
                    Title: "AB tests",
                    Subject: "ab-test",
            };
            
            wb.SheetNames.push("Metrix");
            var ws_data = [['Название метрики' , 'Тестовая группа', 'Контрольная группа']];

            metrix = JSON.parse(returnedData.name).metrix


            for (i = 0; i < metrix.length; i++){
                var data = metrix[i]
                arr = [data['name'], data['test_group'], data['control_group']]
                ws_data.push(arr)
            }

            var ws = XLSX.utils.aoa_to_sheet(ws_data);
            wb.Sheets["Metrix"] = ws;
        
            var wbout = XLSX.write(wb, {bookType:'xlsx',  type: 'binary'});
            function s2ab(s) {
        
                    var buf = new ArrayBuffer(s.length);
                    var view = new Uint8Array(buf);
                    for (var i=0; i<s.length; i++) view[i] = s.charCodeAt(i) & 0xFF;
                    return buf;
                    
            }
            
            saveAs(new Blob([s2ab(wbout)],{type:"application/octet-stream"}), 'ab-test.xlsx');
            


            $("#result").html(returnedData.name)
      },
    });                
}

/*
var wb = XLSX.utils.book_new();
wb.Props = {
    Title: "SheetJS Tutorial",
    Subject: "Test",
    Author: "Red Stapler",
    CreatedDate: new Date(2017,12,19)
};


wb.SheetNames.push("Test Sheet");
var ws_data = [['hello' , 'world']];
var ws = XLSX.utils.aoa_to_sheet(ws_data);
wb.Sheets["Test Sheet"] = ws;

var wbout = XLSX.write(wb, {bookType:'xlsx',  type: 'binary'});
function s2ab(s) {

    var buf = new ArrayBuffer(s.length);
    var view = new Uint8Array(buf);
    for (var i=0; i<s.length; i++) view[i] = s.charCodeAt(i) & 0xFF;
    return buf;
    
}
$("#button-a").click(function(){
    saveAs(new Blob([s2ab(wbout)],{type:"application/octet-stream"}), 'test.xlsx');
});
*/
