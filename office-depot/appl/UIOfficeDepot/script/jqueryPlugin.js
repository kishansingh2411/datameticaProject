//create Plugin For Jquery for morrisCharts
//Siddharth Chandra For To Be Used As a Jquery Plugin
//Must Be Called For ready or onload

/*Func to Print Numbers with comma seperated

new Intl.NumberFormat().format()

*/

$(document).ready(function() {
    $('#drop').click(function(event) {
       //console.log(event);  
      alert('hi');
         var aa = ($('#drop option:selected').text());  

         //console.log(aa);    
            });


});


(function($)
 {
 
    log = console.log.bind(console);
  
    error = console.log.bind(console);

 	var pathArray = window.location.pathname.split( '/' );
	var len = pathArray.length;
	var html = pathArray[len-1];


    $.roundToTwo  = function(num)
    {    
      return +(Math.round(num + "e+1")  + "e-1");
    };

  



    
    $.formatToJSON = function(datatest)
    {
      try{
        var array ={};
        var list = {};
            list.dataSet =[];
        var len = datatest.columnHeaders.length;
        var rows = datatest.rows.length;
    
        var j=0;
        while(j!=rows)
        {    
          var i=0;
          var array ={};
          while(i!=len)
         {   
    
          var name = datatest.columnHeaders[i].name;
          var pair   =datatest.rows[j][i];
        
          array[name] = pair;
          i+=1;  
         } 
         list.dataSet.push(array);
         j+=1; 
       } 

       
        return list;

     return this;
    }
    catch(e)
    {
     // log("Error");
      //log(e.message);
    }

    };







$.getStructuredDataFunnel = function(data)
{
     try{

        var loop=0;
        var shoppingCartStartedSum=0;
        var totalOrdersSum=0;
        var totalViewedProductsSum=0;
        var totalVisitsSum=0;
        while(loop!=data.length)
        {
            shoppingCartStartedSum = shoppingCartStartedSum+parseInt(data[loop].shoppingCartStarted);
            totalOrdersSum  = totalOrdersSum+parseInt(data[loop].totalOrders);
            totalViewedProductsSum = totalViewedProductsSum+parseInt(data[loop].totalViewedProducts);
            totalVisitsSum = totalVisitsSum+parseInt(data[loop].totalVisits);
          loop+=1;  
        }

        // var shop  = parseInt(shoppingCartStartedSum/1000000);
        // var orderSum = parseInt(totalOrdersSum/1000000);
        // var viewedPro = parseInt(totalViewedProductsSum/1000000);
        // var totalVisit = parseInt(totalVisitsSum/1000000);
        var shop  = parseInt(shoppingCartStartedSum);
        var orderSum = parseInt(totalOrdersSum);
        var viewedPro = parseInt(totalViewedProductsSum);
        var totalVisit = parseInt(totalVisitsSum);

        //log(shop,orderSum,viewedPro,totalVisit);
         shopGlobal=shop;
         orderSumGlobal=orderSum;
         viewedProGlobal=viewedPro;
         totalVisitGlobal=totalVisit;
      //   log(shopGlobal,orderSumGlobal,viewedProGlobal,totalVisitGlobal);

     //Now After Getting SUm Link With Key Value Pair  Pass Data To Morris
    //Create DATA For chart.js
    var data = 
     {
     labels: ["Visited", "Viewed Product", "Placed Product In Carts", "Placed Order"],
     datasets: 
     [
       {
             label: "Funnel View",
             fillColor: "#337ab7",
              strokeColor: "rgba(220,220,220,0.8)",
              highlightFill: "#D9534F",
              highlightStroke: "rgba(220,220,220,1)",
             data: [totalVisit,viewedPro,shop,orderSum]
          }
     ]
     };

     //1st- totalVisitsSum
     //2nd- totalViewedProductsSum
     //3rd-
   
     return data;

     return this;

   }
   catch(e)
   {
    //log("Error :");
    //log(e);
   }

};

    $.load = function(url,success,failure)
    {
        $.ajax(
         {
            url:url,
            type : "GET",
            async :true,     
            success: success,
            error: failure
         });  
    	return this;
    };









    

  $.getCurrentWeek_Ly = function(){
 Date.prototype.getWeek = function() {
      var onejan = new Date(this.getFullYear(), 0, 1);
      return Math.ceil((((this - onejan) / 86400000) + onejan.getDay() + 1) / 7);
  }
  var now = new Date();
  var y = now.getFullYear()-1;
  var week = ((new Date()).getWeek()-1);

  var weekNumber = (y.toString() + "W" + week.toString());
  return weekNumber;
};

$.getWeek_Ly = function(backWeekupTo){

    // Date.prototype.get_Week = function() {
    //     var onejan = new Date(this.getFullYear(), 0, 1);
    //     return Math.ceil((((this - onejan) / 86400000) + onejan.getDay() + 1) / 7);
    // }

    // var weekNumber = (new Date()).get_Week();

    // var dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    // var now = new Date();
    // var y = now.getFullYear()-1;

    // weekrange = (y.toString() + "W" + (weekNumber-backWeekupTo));

    // return weekrange;
    // return this;

    var ONE_DAY = 24 * 60 * 60 * 1000;

      var getWeek = function(d) {
      var onejan = new Date(d.getFullYear(), 0, 1);
      return Math.ceil((((d - onejan) / 86400000) + onejan.getDay() + 1) / 7);
      };

      var subtractWeeks = function(weeks) {
      var weeksAsDays = weeks * 7;
      var today = new Date();
      var pastWeek = new Date(today.getFullYear(), today.getMonth(), today.getDate() - weeksAsDays);
      return pastWeek;
      };

      var getWeekRange = function(d){
      var weekNumber = getWeek(d);
      var y = d.getFullYear()-1;
      var weekrange = (y.toString() + "W" + weekNumber);
      return weekrange;
      };

      return getWeekRange(subtractWeeks(backWeekupTo));
};


$.getCurrentWeek = function(){
   Date.prototype.getWeek = function() {
        var onejan = new Date(this.getFullYear(), 0, 1);
        return Math.ceil((((this - onejan) / 86400000) + onejan.getDay() + 1) / 7);
    }
    var now = new Date();
    var y = now.getFullYear();
    var week = ((new Date()).getWeek()-1);

    var weekNumber = (y.toString() + "W" + week.toString());
    return weekNumber;
}

$.getlastWeek = function(){
   Date.prototype.getWeek = function() {
        var onejan = new Date(this.getFullYear(), 0, 1);
        return Math.ceil((((this - onejan) / 86400000) + onejan.getDay() + 1) / 7);
    }
    var now = new Date();
    var y = now.getFullYear();
    var week = ((new Date()).getWeek()-1);

    var weekNumber = week.toString();
    return weekNumber;
}

$.getWeek = function(backWeekupTo){

    // Date.prototype.get_Week = function() {
    //     var onejan = new Date(this.getFullYear(), 0, 1);
    //     return Math.ceil((((this - onejan) / 86400000) + onejan.getDay() + 1) / 7);
    // }

    // var weekNumber = (new Date()).get_Week();

    // var dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    // var now = new Date();
    // var y = now.getFullYear();

    // weekrange = (y.toString() + "W" + (weekNumber-backWeekupTo));

    // return weekrange;
    // //return this;

      var ONE_DAY = 24 * 60 * 60 * 1000;

      var getWeek = function(d) {
      var onejan = new Date(d.getFullYear(), 0, 1);
      return Math.ceil((((d - onejan) / 86400000) + onejan.getDay() + 1) / 7);
      };

      var subtractWeeks = function(weeks) {
      var weeksAsDays = weeks * 7;
      var today = new Date();
      var pastWeek = new Date(today.getFullYear(), today.getMonth(), today.getDate() - weeksAsDays);
      return pastWeek;
      };

      var getWeekRange = function(d){
      var weekNumber = getWeek(d);
      var y = d.getFullYear();
      var weekrange = (y.toString() + "W" + weekNumber);
      return weekrange;
      };

      return getWeekRange(subtractWeeks(backWeekupTo));
};


    $.tableDataDisplay = function(data)
    { 
      try
      {
        
      //var commontableData ={}; 
        var formData = this.formatToJSON(data);
         //console.log(JSON.stringify(formData));
        return formData;
        return this;

      }
      catch(e)
      {
        //log("Error : ");
        //log(e);
      }
    };


    $.getWkRange = function()
      { 
        try
          {

      var week_range = $('input:radio[name=weekrange]:checked').val();
      return week_range;
      //return this;
      }
      catch(e)
      {
        log("Error : ");
        log(e);
      }
    };


    $.filtered1 = function(values)
      { 
        try
          {

      var selected_visitor_seg = $('input:radio[name=visitorSeg]:checked').val();
      alert(selected_visitor_seg);

      var returned_header = $.grep(values.dataSet, function(element, index){
            return  element.visitorSegment == selected_visitor_seg;
      });

      return returned_header;
      //return this;
      }
      catch(e)
      {
        log("Error : ");
        log(e);
      }
  };

function countryChk(){
     var sel_country = undefined;
         $('select').on('change', function () {
        //console.log("there");
          var optionSelected = $(this).find("option:selected");
          //var valueSelected  = optionSelected.val();
          //var textSelected   = optionSelected.text();
          });
         alert(sel_country);

};


  $.countryFilter = function(data){

    var optionSelected = $(this).find("option:selected");
      var returned_country_header = $.grep(data.dataSet, function(element, index){
            return  element.country == optionSelected;
          });
     
          return returned_country_header;

  };



 $.filteredname = function(values,valdata){
          //console.log(values,valdata)
          var togle = valdata;
        
          if( togle == "All Visitors"){

            //alert("yes All");
            var returned_header = values.dataSet;
            //return returned_header
          }
          else {
            // alert("not all selected");

            var selected_visitor_seg = valdata;
            //alert(selected_visitor_seg);
             // console.log("me" + aa);

          var returned_header = $.grep(values.dataSet, function(element, index){
                  return  element.visitorSegment == selected_visitor_seg && element.country == returned_header;
          });
          return returned_header;
          }
            return returned_header;
          
  };



  $.filteredCountry = function(values){

          var sel_country = $('#countries').find(":selected").text();

          if(sel_country == "All COUNTRIES"){
              var returned_country = values.dataSet;
          }
          else {            
            var returned_country = $.grep(values.dataSet, function(element, index){
              return  element.country == sel_country;
            });
            return returned_country;
          }
            return returned_country;
         
          
  };
  $.filtered = function(values){

          //var togle = $('input:radio[name=visitorSeg]:checked').val();
          
          var sel_visitor = $('#Visitor').find(":selected").text();
          
          if( sel_visitor == "ALL VISITORS"){

            //alert("yes All");
            var returned_header = values;
            //return returned_header
          }
          else {

          var returned_header = $.grep(values, function(element, index){
                  return  element.visitorSegment == sel_visitor;
          });
          return returned_header;
           }
          return returned_header;
          
  };

  $.filteredWeeks = function(datasrc){
      var sel_period = $("#radio-div input[type='radio']:checked").val();
       var prevWk = $.getWeek(2);
        var prevWk2 = $.getWeek(3);
        var prevWk3 = $.getWeek(4);
        var prevWk4 = $.getWeek(5);
        var prevWk5 = $.getWeek(6);
        var prevWk6 = $.getWeek(7);
        var prevWk_Ly = $.getWeek_Ly(2); //last year week
        var prevWk_Ly2 = $.getWeek_Ly(3);
        var prevWk_Ly3 = $.getWeek_Ly(4);
        var prevWk_Ly4 = $.getWeek_Ly(5);
        var prevWk_Ly5 = $.getWeek_Ly(6);
        var prevWk_Ly6 = $.getWeek_Ly(7);
        var weeksStoreCurrent = [prevWk, prevWk2, prevWk3, prevWk4, prevWk5, prevWk6];
        var weeksStoreLast = [prevWk_Ly, prevWk_Ly2, prevWk_Ly3, prevWk_Ly4, prevWk_Ly5, prevWk_Ly6];
      
      if(sel_period == 'Prior Week'){
        
        var returned_period = $.grep(datasrc, function(element, index){
                  return  element.week == prevWk || element.week == prevWk_Ly;
          });
         return returned_period;
        //console.log(returned_period);
      }
      else if(sel_period == '6 Weeks Avg'){         
        var returned_period = $.grep(datasrc, function(element, index){

              return  element.week == prevWk || element.week == prevWk2 || element.week == prevWk3 || element.week == prevWk4 || element.week == prevWk5 || element.week == prevWk6 || element.week == prevWk_Ly || element.week == prevWk_Ly2 || element.week == prevWk_Ly3 || element.week == prevWk_Ly4 || element.week == prevWk_Ly5 || element.week == prevWk_Ly6;
                // for (var i=0, j=0; i<weeksStoreCurrent.length && j<weeksStoreLast.length; i++,j++)    { 
                //   return  element.week == weeksStoreCurrent[i] || element.week == weeksStoreLast[j];
                // };
               
          });
        return returned_period;        
      }         
     
      else if(sel_period == '13 Weeks Avg') {
        
        var returned_period = datasrc;
      }
      return returned_period;
  };

  $.getDatafortable = function(datasrc){      
      var objj = {};
      objj.TR = 0;
      objj.TO = 0;
      objj.CA = 0;
      objj.TV = 0;     
     
      for(var i=0; i<datasrc.length; i++){
        //var v = datasrc[i].totalRevenue;
        //objj.CYCW_TR += v;
        objj.TR += parseInt(datasrc[i].totalRevenue);
        objj.TO += parseInt(datasrc[i].totalOrders);
        objj.CA += parseInt(datasrc[i].cartAbandonment);
        objj.TV += parseInt(datasrc[i].totalPageViews);
      }
      return objj;
  };

  $.getDataforKPI1 = function(datasrc){
    var objj = {};
    objj.to = 0;
    objj.tv = 0;
    for(var i=0; i<datasrc.length; i++){
        objj.to += parseFloat(datasrc[i].totalOrders);
        objj.tv += parseFloat(datasrc[i].totalVisits);
      }
    var kpi_val = (objj.to/objj.tv).toFixed(3);
    return kpi_val; 
  };

  $.getDataforKPI2 = function(datasrc){
    var objj = {};
    objj.one = 0;
    objj.two = 0;
    for(var i=0; i<datasrc.length; i++){
        objj.one += parseFloat(datasrc[i].totalRevenue);
        objj.two += parseFloat(datasrc[i].totalOrders);
      }
    var kpi_val = (objj.one/objj.two).toFixed(3);
    return kpi_val; 
  };

   $.getDataforKPI3 = function(datasrc){
    var objj = {};
    objj.one = 0;
    objj.two = 0;
    for(var i=0; i<datasrc.length; i++){
        objj.one += parseInt(datasrc[i].totalPageViews);
        objj.two += parseInt(datasrc[i].totalVisits);
      }
    var kpi_val = (objj.one/objj.two).toFixed(3);
    return kpi_val; 
  };

  
  $.getDataforKPI4a = function(datasrc){
    var objj = {};
    objj.one = 0;
    objj.two = 0;
    for(var i=0; i<datasrc.length; i++){
        objj.one += parseInt(datasrc[i].totalRevenueFromNewCustomers);
        objj.two += parseInt(datasrc[i].totalRevenue);
      }
    var kpi_val = (objj.one/objj.two).toFixed(3);
    return kpi_val; 
  };

  $.getDataforKPI4b = function(datasrc){
    var objj = {};
    objj.one = 0;
    objj.two = 0;
    for(var i=0; i<datasrc.length; i++){
        objj.one += parseInt(datasrc[i].totalRevenueFromExistingCustomers);
        objj.two += parseInt(datasrc[i].totalRevenue);
      }
    var kpi_val = (objj.one/objj.two).toFixed(3);
    return kpi_val; 
  };

  $.getDataforKPI5 = function(datasrc){
    var objj = {};
    objj.one = 0;
    objj.two = 0;
    for(var i=0; i<datasrc.length; i++){
        objj.one += parseInt(datasrc[i].totalOrders);
        objj.two += parseInt(datasrc[i].shoppingCartStarted);
      }
    var kpi_val = (objj.one/objj.two).toFixed(3);
    return kpi_val; 
  };


  $.discreteBarChartKpi=function(divbox,  kpival){
       console.log('find exact div'+divbox +  JSON.stringify(kpival));
 
        d3.selectAll(divbox).selectAll("svg").remove();

        nv.addGraph(function() {
            var data1 = [{
                key: "",
                values:kpival

            }]
            var TotalCount = nv.models.discreteBarChart()
                .width(500)
                .height(270)
                .margin({bottom: 60})


                .x(function(d) {
                    return d.label
                })
                .y(function(d) {
                    return d.value
                })
                .tooltips(false)
                .staggerLabels(false)    //Too many bars and not enough room? Try staggering labels.
                //Don't show tooltips
                .showValues(false)       //...instead, show the bar value right on top of each bar.
                .transitionDuration(350)
                .showValues(true).color(['#a8324e', '#7a3854', '#484fb8', '#393e87']);//(['#c32626', '#822c2c', '#4343b3','#32326d'])


                //  #kpi1barFinalChart > svg > g > g > g.nv-x.nv-axis > g > g > g:nth-child(1) > text

                // var xTicks = d3.select('svg > g > g > g.nv-x.nv-axis > g > g').selectAll('g');
                // xTicks
                //   .selectAll('text')
                //   .attr('transform', function(d,i,j) { return 'translate (-10, 25) rotate(-90 0,0)' }) ;

                 TotalCount.xAxis.rotateLabels(-45);

            d3.select(divbox).append("svg")
                .datum(data1)
                .transition().duration(500)
                .call(TotalCount);
            nv.utils.windowResize(TotalCount.update);
            return TotalCount;
        });
      }


    $.funneDataChart = function(data, id, title, wkno){

          var totalVisit = data.datasets[0].data[0]/wkno;
                var viewedPro = data.datasets[0].data[1]/wkno;
                var shopD = data.datasets[0].data[2]/wkno;
                var orderSum = data.datasets[0].data[3]/wkno;

                var visP =100+"%";
                var viP =  $.roundToTwo(Math.round(viewedPro/totalVisit*100))+"%";
                var pP = $.roundToTwo(Math.round(shopD/totalVisit*100))+"%";
                var pO =  $.roundToTwo(Math.round(orderSum/totalVisit*100))+"%";
                                    
                  $(id).highcharts({
                      chart: {
                          type: 'funnel',
                          marginRight: 100
                      },
                      title: {
                          text: title,
                          x: -40
                      },
                      plotOptions: {
                          series: {
                              dataLabels: {
                                  enabled: true,
                                  format: '<b>{point.name}</b> ({point.y:,.0f})',
                                  color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black',
                                  softConnector: true
                              },
                              neckWidth: '2%',
                              neckHeight: '0%'
                          }
                      },
                      credits: {      // remove highchart.com link label from chart.
                             enabled: false
                      },
                      legend: {
                          enabled: false
                      },
                      series: [{
                          name: 'Unique users',
                          data: [
                                  ['Visited ' + visP,   totalVisit],
                                  ['Viewed Products ' + viP, orderSum],
                                  ['Placed Products in cart ' + pP, shopD],
                                  ['Placed Orders ' + pO,    orderSum]
                                  
                              ]
                      }]
                      });

    };

    

        $.populateDropDownCountry = function(popId,dataCountry)
        {

        var loop=0;
        var countryList = [];
        countryList.push("ALL");
        while(loop!=dataCountry.dataSet.length)
        {

        var country = dataCountry.dataSet[loop].country;
        $(popId).append('<li role="presentation"><a role="menuitem" tabindex="-1" href="#/">'+country+"</a></li>");
        countryList.push(country);
        loop+=1; 
        }

        return countryList;
    };


    

         


  }(jQuery));

//Decides Which Page Is Loaded and Load Common Data


