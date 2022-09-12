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

            if (returnedData.error == 1) {
                $("#result").html(returnedData.name)
                return
            }
            
            var wb = XLSX.utils.book_new();
            wb.Props = {
                    Title: "AB tests",
                    Subject: "ab-test",
            };
            
            //wb.SheetNames.push("Metrix");
            //wb.SheetNames.push("Топ запросов");
            //var ws_data = [['Название метрики' , 'Тестовая группа', 'Контрольная группа']];

            console.log(returnedData)
            metrix = JSON.parse(returnedData.name)
            console.log(metrix)
            const worksheet = XLSX.utils.json_to_sheet(metrix);
            
            XLSX.utils.book_append_sheet(wb, worksheet, "Dates");

            //for (i = 0; i < metrix.length; i++){
            //    var data = metrix[i]
            //   arr = [data['metrics'], data['type_A'], data['type_B']]
             //   ws_data.push(arr)
            //}

            //var ws = XLSX.utils.aoa_to_sheet(ws_data);
           //wb.Sheets["Metrix"] = ws;
        
            var wbout = XLSX.write(wb, {bookType:'xlsx',  type: 'binary'});
            function s2ab(s) {
        
                    var buf = new ArrayBuffer(s.length);
                    var view = new Uint8Array(buf);
                    for (var i=0; i<s.length; i++) view[i] = s.charCodeAt(i) & 0xFF;
                    return buf;
                    
            }
            
            saveAs(new Blob([s2ab(wbout)],{type:"application/octet-stream"}), 'ab-test.xlsx');
        
      },
    });                
}


function top_request(){
    console.log('test')
}

function all_number(){
    console.log('test')
   
    var date_from = document.getElementById("date_from").value;
    var date_before = document.getElementById("date_before").value;

    $.ajax({
        type: "POST",
        url: "http://10.1.241.41:9300/all_number",
        data: {"date_from": date_from, "date_before":date_before},  
        success: function (data) { 
            var returnedData = JSON.parse(data);

            if (returnedData.error == 1) {
                $("#result").html(returnedData.name)
                return
            }
            
            var wb = XLSX.utils.book_new();
            wb.Props = {
                    Title: "AB tests",
                    Subject: "ab-test",
            };
            
            //wb.SheetNames.push("Metrix");

            //var ws_data = [['Название метрики' , 'Тестовая группа', 'Контрольная группа']];

            console.log(returnedData)
            console.log(0)
            //metrix = JSON.parse(returnedData.name)
            metrix = returnedData.name
            console.log(1)
            console.log(metrix)
            

            metrix_good = []
            date = ''
            newDate = ''
            date_arr = []



          
            for (i in metrix) {
                if (metrix[0].date == metrix[i].date){
                    nem_elem = {}
                    name_metrics = metrix[i].metrics
                    nem_elem['metrics'] = metrix[i]['metrics']
                    for (j in metrix) {
                        if (metrix[j].metrics == name_metrics){
                            nem_elem[metrix[j].date] = metrix[j]['value']
                        }
                    }
                    metrix_good.push(nem_elem)
                }
                
            }
            
            
            const worksheet = XLSX.utils.json_to_sheet(metrix_good);
            XLSX.utils.book_append_sheet(wb, worksheet, "Dates");
             
            //date = metrix[0].date

            /*for (i = 0; i < metrix.length; i++){
                if (date == metrix[i].date) {

                } else {
                    date = metrix[i].date
                }
                var data = metrix[i]
                arr = [data['metrics'], data['type_A'], data['type_B']]
                ws_data.push(arr)
            }

            var ws = XLSX.utils.aoa_to_sheet(ws_data);
            wb.Sheets["Metrix"] = ws;
        */
            var wbout = XLSX.write(wb, {bookType:'xlsx',  type: 'binary'});
            function s2ab(s) {
        
                    var buf = new ArrayBuffer(s.length);
                    var view = new Uint8Array(buf);
                    for (var i=0; i<s.length; i++) view[i] = s.charCodeAt(i) & 0xFF;
                    return buf;
                    
            }
            
            saveAs(new Blob([s2ab(wbout)],{type:"application/octet-stream"}), 'ab-test.xlsx');
        
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
