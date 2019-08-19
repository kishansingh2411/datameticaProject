(function($)
	{


         /*For Individual Country Selection This MEthod Is Called*/
     $.structCommonPercentRevnNewVisitors =function(data)
    {
    //  log(data);
      try{
       var formatedData = new Array();
       $(data.dataSet).each(function()
     {
       var currentData = this;
       var detail = 
       {"date": currentData.date,
        "totalRevenueFromNewCustomers": currentData.totalRevenueFromNewCustomers,
        "totalRevenue": currentData.totalRevenue,
        "visitorSegment": currentData.visitorSegment
        };
         var newData = jQuery.grep(formatedData,function(a){
         return a.country == currentData.country;
    });
    if(newData.length==0)
    {
    newData = {country: this.country, data: [detail]};
    formatedData.push(newData);
    }    
    else
    {
     newData[0].data.push(detail);
    }


    });
//       log("Data By Country Formatted");
   //   log(formatedData);
      return formatedData;


    }
    catch(e)
    {
      log("Error :");
      log(e);
    }
  };




 $.getStructPercentRevNewVisitors= function(dataCompute)
{
  
  
  try{
  //Chart Value Decleration
  var chart ={};
  chart.labels =[];
  chart.datasets =[];
  //Chart Decleration Ends

  var baseLen = dataCompute.length;
 
/*//Compute Date Length
 var loop=0;
  var dataLoop =0;
 //Given Current Date and Time Are Same For ALL Thus selecting Only 1 For Display
while(loop!=baseLen)
  {  
    var baseDate = dataCompute[loop].data[0].date;
    chart.labels.push(baseDate);
    loop+=1;
  }
 
  var loop=0;
  var dataLoop =0;
  var baseDate = dataCompute[0].data[0].date;
  chart.labels.push(baseDate);*/


  //Add Date
        var  len = dataCompute[0].data.length;
      //  alert(len);
        var loop=0;
        while(loop!=len)
        {
          var date = dataCompute[0].data[loop].date;
          chart.labels.push(date);
          loop+=1;
         }




 var loop=0;
  var dataLoop =0;
  //log(dataCompute);
  while(loop!=baseLen)
  {
     
     var getDataLen = dataCompute[loop].data.length;

     var country =dataCompute[loop].country;
     var dataLoop =0;

     var tempObj ={}; 
     tempObj.label = country;
     tempObj.fillColor = 'rgba(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() *0.9))+')';
     tempObj.strokeColor ='rgb(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ')';
/*     tempObj.pointColor = 'rgba(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ')';
*/   
/*  tempObj.pointStrokeColor = 'rgb(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) +')';
*/
     tempObj.pointHighlightFill ='rgb(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) +')';
     tempObj.pointHighlightStroke ='rgb(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) +',)';
     tempObj.data =[];
      while(dataLoop!=getDataLen)
      {
      
        /*var date = dataCompute[loop].data[dataLoop].date;
        chart.labels.push(date);
*/

        var totalRevenueFromNewCustomers =parseFloat(dataCompute[loop].data[dataLoop].totalRevenueFromNewCustomers);
        var totalRevenue = parseFloat(dataCompute[loop].data[dataLoop].totalRevenue);
/*        var visitorSegment =parseFloat(dataCompute[loop].data[dataLoop].visitorSegment);
*/        
        //OCR Compute totalOrders/totalVisits
        var avgPro =totalRevenueFromNewCustomers/totalRevenue;
        log("Perform Value");
        log(avgPro);

          if(avgPro.toString()=="NaN")
        {
          avgPro=0;
        }



        // ocrGlobal = ocr;
     //   tempObj.data.push(Math.round(ocr*100)/100);
         tempObj.data.push(avgPro);

        dataLoop+=1;
      }  
      
      chart.datasets.push(tempObj);
   
    loop+=1;
  }
  //log(chart);
  return chart;
}
catch(e)
{
    log("Error : "+e);
    log(e);
}
};





   /* get Country Based on DropDown Selection*/
    $.displayByCountryChartPercentRevNewVisitors=function(countryOrder,formattedDataGlobal)
    {
      
     // log(globalChartData);
     // log(formattedDataGlobal);
      var matchedCountry;
      //match Data 
      var loop=0;
      while(loop!=formattedDataGlobal.length)
      {

        if(countryOrder.country ==formattedDataGlobal[loop].country)
        {
          matchedCountry = formattedDataGlobal[loop];
          break;
        }
        loop+=1;
      }
       
      //Build Data For Chart
        var chart ={};
        chart.labels =[];
        chart.datasets =[];


         var loop=0;
         var tempObj ={}; 
         tempObj.label = countryOrder.country;
         tempObj.fillColor = 'rgba(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) +','+(Math.floor(Math.random() *0.9)) +')';
         tempObj.strokeColor ='rgb(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ')';
/*         tempObj.pointColor = 'rgba(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ')';
*/
/*         tempObj.pointStrokeColor = 'rgb(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) +')';
*/         tempObj.pointHighlightFill ='rgb(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) +')';
         tempObj.pointHighlightStroke ='rgb(' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) + ',' + (Math.floor(Math.random() * 256)) +',)';
         tempObj.data =[];
      
         var getCountryDatalength = matchedCountry.data.length;
         //Get Date Difference Based on Intelligence
         var diffReturns = this.diffDates(countryOrder.timeFrom,countryOrder.timeTo,getCountryDatalength);
         
         

         /* 

         var getDate=0;
         while(getDate!= matchedCountry.data.length)
         {
            alert("Range Date : "+matchedCountry.data[getDate].date);

            getDate+=1;
         }


        chart.labels.push(countryOrder.timeFrom);
         chart.labels.push(countryOrder.timeTo);*/

         //create Date buffer

        while(loop!= matchedCountry.data.length)
        {  
          /*Get Dates*/
          chart.labels.push(matchedCountry.data[loop].date);   //Has to
          /*Ends*/
         var totalRevenueFromNewCustomers =parseFloat(matchedCountry.data[loop].totalRevenueFromNewCustomers);
         var totalRevenue = parseFloat(matchedCountry.data[loop].totalRevenue);
         //OCR Compute totalOrders/totalVisits
          var ocr =totalRevenue/totalRevenueFromNewCustomers;  

          if(ocr.toString()=="NaN")
          {
            ocr=0;
          }
          
          ocrGlobal[loop] = ocr;
         // tempObj.data.push((Math.round(ocr * 100) / 100));
         tempObj.data.push(ocr);
          loop+=1;
        }
        chart.datasets.push(tempObj);

      //  log(chart);

      return chart;

    }



	})(jQuery)