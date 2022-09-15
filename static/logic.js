function cmd(){
    //var valueAB = document.getElementById("selectAB").value;
    var date_from = document.getElementById("date_from").value;
    var date_before = document.getElementById("date_before").value;
    var A_start = document.getElementById("A_start").value;
    var A_end = document.getElementById("A_end").value;
    var B_start = document.getElementById("B_start").value;
    var B_end = document.getElementById("B_end").value;

    $.ajax({
        type: "POST",
        url: "http://10.1.241.41:9300/cmd",
        data: {"idAB": 0, "date_from": date_from, "date_before":date_before,"A_start": A_start, "A_end": A_end, "B_start": B_start, "B_end": B_end},  
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
    console.log('top request')
   
    

    //document.getElementById("result").innerHTML  = 'тестовый запрос'

    var date_from = document.getElementById("date_from").value;
    var date_before = document.getElementById("date_before").value;
    var A_start = document.getElementById("A_start").value;
    var A_end = document.getElementById("A_end").value;
    var B_start = document.getElementById("B_start").value;
    var B_end = document.getElementById("B_end").value;

    var frequency = document.getElementById("frequency").value;
    var count_add = document.getElementById("count_add").value;
    var frequency_max = document.getElementById("frequency_max").value;
    var count_add_max = document.getElementById("count_add_max").value;
    var select_delta = document.getElementById("select_delta").value;

   // console.log(frequency)
  //console.log(Number.isNaN(frequency))

    if (frequency <= 0 || frequency == '') {
        frequency = 1
        document.getElementById("frequency").value = '1'
    }
    if (count_add <= 0 || count_add == '') {
        count_add = 1
        document.getElementById("count_add").value = '1'
    }
    if (frequency_max <= 0 || frequency_max == '') {
        frequency_max = 1000000
        document.getElementById("frequency_max").value = '1000000'
    }
    if (count_add_max <= 0 || count_add_max == '') {
        count_add_max = 1000000
        document.getElementById("count_add_max").value = '1000000'
    }



    $.ajax({
        type: "POST",
        url: "http://10.1.241.41:9300/top_request",
        data: {"date_from": date_from, "date_before":date_before, "A_start": A_start, "A_end": A_end, "B_start": B_start, "B_end": B_end},  
        success: function (data) { 
            console.log('0')
            var returnedData = JSON.parse(data);

           

            console.log('1')
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
            
            console.log('returnedData')
            console.log(returnedData)
            console.log(2)
            //metrix = JSON.parse(returnedData.name)
            metrix = JSON.parse(returnedData.name)
            console.log(3)
            console.log(metrix)
            
            metrix_good_1 = []
            metrix_good_2 = []
            all_pages = []
            date = ''
            newDate = ''
            date_arr = []

          
            /*for (i in metrix) {
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
                
            }*/

            select = 'delta_avg'

            if (select_delta == 'Дельта конверсии') {
                select = 'delta_konv'
            } 

            
            all_pages = {}
            max_len = 1
            index = 0
            console.log(4)
            for (i in metrix){
                if (count_add_max >= metrix[i]['add_all'] && frequency_max >= metrix[i]['search_all_']  && frequency <= metrix[i]['search_all_'] && count_add <= metrix[i]['add_all'] && metrix[i][select] > 0 && max_len < 300000) {
                    nem_elem = {}
                    nem_elem['Запрос'] = metrix[i]['search_bar']

                    nem_elem['Количество карт общее'] = metrix[i]['number_all_']
                    nem_elem['Частотность запросов общая'] = metrix[i]['search_all_']
                    nem_elem['Количество выдач с добавлением общее'] = metrix[i]['add_all']

                    nem_elem['А Количество клиентов'] = metrix[i]['cout_numbers_x']
                    nem_elem['А количество выдач'] = metrix[i]['cout_id_search_x']
                    nem_elem['А количество выдач с добавлением'] = metrix[i]['search_bar_add_all__x']
                    nem_elem['А конверсия = количество выдач с добавлением/количество выдач'] = metrix[i]['konv__x']
                    nem_elem['А средняя позиция добавления'] = metrix[i]['avg_position_x']
                    nem_elem['А среднее количество товаров в выдаче'] = metrix[i]['avg_rn_max_x']

                    nem_elem['Б Количество клиентов'] = metrix[i]['cout_numbers_y']
                    nem_elem['Б количество выдач'] = metrix[i]['cout_id_search_y']
                    nem_elem['Б количество выдач с добавлением'] = metrix[i]['search_bar_add_all__y']
                    nem_elem['Б конверсия = количество выдач с добавлением/количество выдач'] = metrix[i]['konv__y']
                    nem_elem['Б средняя позиция добавления'] = metrix[i]['avg_position_y']
                    nem_elem['Б среднее количество товаров в выдаче'] = metrix[i]['avg_rn_max_y']
                    
                    nem_elem['дельта конверсии отбор по максимальному значению только положительные'] = metrix[i]['delta_konv']
                    nem_elem['дельта средней позиции отбор по максимальному значению только положительные'] = metrix[i]['delta_avg']
                    
                    max_len = max_len + 1

                    //index = Math.floor(max_len/100000)

                    if (max_len < 150000) {
                        metrix_good_1.push(nem_elem)
                    } else {
                        metrix_good_2.push(nem_elem)
                    }
                    
                }
            }

      
            console.log(5)
       
            const worksheet = XLSX.utils.json_to_sheet(metrix_good_1);
            XLSX.utils.book_append_sheet(wb, worksheet, 'Metric');

         

            console.log(6)
            var wbout = XLSX.write(wb, {bookType:'xlsx',  type: 'binary'});

            function s2ab(s) {
        
                    var buf = new ArrayBuffer(s.length);
                    var view = new Uint8Array(buf);
                    for (var i=0; i<s.length; i++) view[i] = s.charCodeAt(i) & 0xFF;
                    return buf;
                    
            }

            console.log(7)

            saveAs(new Blob([s2ab(wbout)],{type:"application/octet-stream"}), 'ab-test.xlsx');
            
            console.log(8)

            /*
            if (max_len > 150000) {

                var wb_1 = XLSX.utils.book_new();
                    wb_1.Props = {
                        Title: "AB tests",
                        Subject: "ab-test",
                    };

                const worksheet_1 = XLSX.utils.json_to_sheet(metrix_good_2);
                XLSX.utils.book_append_sheet(wb_1, worksheet_1, 'Metric');
                var wbout = XLSX.write(wb, {bookType:'xlsx',  type: 'binary'});
                saveAs(new Blob([s2ab(wbout)],{type:"application/octet-stream"}), 'ab-test.xlsx');
            }
            */
            console.log(9)
        
      },
    });  


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
            metrix = JSON.parse(returnedData.name)
            console.log(1)
            console.log(metrix)
            
            metrix_good = []
            date = ''
            newDate = ''
            date_arr = []

            for (i in metrix) { 
                console.log(i)
            }

            
            for (i in metrix) {
                if (metrix[0].week == metrix[i].week){
                    nem_elem = {}
                    name_metrics = metrix[i].metric
                    nem_elem['metrics'] = metrix[i].metric
                    for (j in metrix) {
                        if (metrix[j].metric == name_metrics){
                            nem_elem[metrix[j].week] = metrix[j].value
                        }
                    }
                    metrix_good.push(nem_elem)
                }
                
            }
            
            console.log(metrix_good)
            
            const worksheet = XLSX.utils.json_to_sheet(metrix_good);
            XLSX.utils.book_append_sheet(wb, worksheet, "Dates");
             
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
