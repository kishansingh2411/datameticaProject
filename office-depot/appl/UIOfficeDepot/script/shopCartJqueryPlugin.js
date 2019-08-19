(function($)
{
	 $.structCommonShopCart =function(data)
    {
    //  log(data);
      try{
       var formatedData = new Array();
       $(data.dataSet).each(function()
     {
       var currentData = this;
       var detail = 
       {"date": currentData.date,
        "totalOrders": currentData.totalOrders,
        "shoppingCartStarted": currentData.shoppingCartStarted,
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

   $.getStructShopCartForChart= function(dataCompute)
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

        var totalOrders =parseFloat(dataCompute[loop].data[dataLoop].totalOrders);
        var shoppingCartStarted = parseFloat(dataCompute[loop].data[dataLoop].shoppingCartStarted);
/*        var visitorSegment =parseFloat(dataCompute[loop].data[dataLoop].visitorSegment);
*/        
        //OCR Compute totalOrders/totalVisits
        var avgPro =totalOrders/shoppingCartStarted;
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



  $.displayByCountryChartShop =function(countryOrder,formattedDataGlobal)
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
         var totalOrders =parseFloat(matchedCountry.data[loop].totalOrders);
         var shoppingCartStarted = parseFloat(matchedCountry.data[loop].shoppingCartStarted);
         var visitorSegment =parseFloat(matchedCountry.data[loop].visitorSegment);
         //OCR Compute totalOrders/totalVisits
          var ocr =totalOrders/shoppingCartStarted;  

          if(ocr.toString()=="NaN")
          {
            ocr=0;
          }
          
          ocrGlobal[loop] = ocr;
          log("ocrGlobal[loop] "+ocrGlobal[loop])
         // tempObj.data.push((Math.round(ocr * 100) / 100));
         tempObj.data.push(ocr);
          loop+=1;
        }
        chart.datasets.push(tempObj);

      //  log(chart);

      return chart;

    };
    //totalOrders/shoppingCartStarted

    /*Format Data for Bar/Stack Chart based on Visitor Segment*/

    $.formatToStackChartDataAvgCart  = function(serialData)
    {
    
      log("Data To Be Compiled For Chart ShoppingCartCompletion");
      log(serialData);
      var loop = 0;

       //Build Data For Chart
        var chart ={};
        chart.labels = [];
        chart.datasets = [];

        var labels =[];
        datasets = [];
        log("Chart Structure");
        log(chart);
        

        /*Individual Data Seets Based on If Data Length >0*/
        dataNew =[];
        dataExist =[];
        dataDefault =[];
        dataExCatCust =[];
        dataNA =[];

        var isDEFAULT = false;
        var isEXISTINGCUSTOMER =false;
        var isNEWCUSTOMER = false;
        var isEXISTINGCATALOGUECUSTOMER = false;
        var isNA = false;
        var timeStampData = null;


        var eventLoop = 0;

       /* if(serialData.NA.length!=0)
        {
          log("NA >0 "+serialData.NA.length);
          isNA =true;
          eventLoop = serialData.NA.length;
          timeStampData = serialData.NA;
        }
        else
        {
          log("NA LESS OR EQUAL TO 0");
        }*/

        if(serialData.DEFAULT.length!=0)
        {
          log("DEFAULT >0 "+serialData.DEFAULT.length);
          isDEFAULT =true;
          eventLoop = serialData.DEFAULT.length;
          timeStampData = serialData.DEFAULT;
        }
        else
        {
          log("DEFAULT LESS OR EQUAL TO 0");
        }
         if(serialData.EXISTINGCUSTOMER.length!=0)
         {
          log("EXISTINGCUSTOMER >0 "+serialData.EXISTINGCUSTOMER.length);
          isEXISTINGCUSTOMER =true;
          eventLoop = serialData.EXISTINGCUSTOMER.length;
          timeStampData = serialData.EXISTINGCUSTOMER;
        }
        else
        {
          log("EXISTINGCUSTOMER LESS OR EQUAL TO 0");
        }
         if(serialData.NEWCUSTOMER.length!=0)
        {
          log("NEWCUSTOMER >0 "+serialData.NEWCUSTOMER.length);
          isNEWCUSTOMER=true;
          eventLoop = serialData.NEWCUSTOMER.length;
          timeStampData = serialData.NEWCUSTOMER;
        }
        else
        {
          log("NEWCUSTOMER LESS OR EQUAL TO 0");
        }
         if(serialData.EXISTINGCATALOGUECUSTOMER.length!=0)
        {
          log("EXISTINGCATALOGUECUSTOMER >0 "+serialData.EXISTINGCATALOGUECUSTOMER.length);
          isEXISTINGCATALOGUECUSTOMER=true;
          eventLoop = serialData.EXISTINGCATALOGUECUSTOMER.length;
          timeStampData = serialData.EXISTINGCATALOGUECUSTOMER;
        }
        else
        {
          log("EXISTINGCATALOGUECUSTOMER LESS OR EQUAL TO 0");
        }



        log("Event Loop " + eventLoop);

      /*Create DataSet For StackedChart*/
      // As ALl serialData dat Length Would be same thus can select anyone
      //stacked chart require 3 dataset with multiple data values to be fed in

      /*Create Time Label*/
      while(loop!=eventLoop)
      {
        labels.push(timeStampData[loop].date);
        loop+=1;
      }

       



      /*Create Chart Data Annoymonus*/

      /*Add OCR VALUES*/
 
      var loop=0
      var OCRSUMS =[];
       while(loop!=eventLoop)
      {
        var ocrExCatCust=0;
        var ocrNew=0;
        var ocrExt=0;
        var ocrDef=0;
        var ocrNA =0;

       /* if(isNA)
        {  
          ocrNA = parseFloat(serialData.NA[loop].totalOrders/serialData.NA[loop].shoppingCartStarted);
        }*/


        if(isDEFAULT)
        {  
          ocrDef = parseFloat(serialData.DEFAULT[loop].totalOrders/serialData.DEFAULT[loop].shoppingCartStarted);
        }
        if(isEXISTINGCATALOGUECUSTOMER)
        {  
          ocrExCatCust = parseFloat(serialData.EXISTINGCATALOGUECUSTOMER[loop].totalOrders/serialData.EXISTINGCATALOGUECUSTOMER[loop].shoppingCartStarted);
        }

        if(isNEWCUSTOMER)
        {  
         ocrNew  = parseFloat(serialData.NEWCUSTOMER[loop].totalOrders/serialData.NEWCUSTOMER[loop].shoppingCartStarted);
        }
        if(isEXISTINGCUSTOMER)
        {  
         ocrExt  = parseFloat(serialData.EXISTINGCUSTOMER[loop].totalOrders/serialData.EXISTINGCUSTOMER[loop].shoppingCartStarted);
        }
        OCRSUMS.push(ocrExCatCust+ocrNew+ocrExt+ocrDef+ocrNA);
        loop+=1;
      }


      /*IF DEFAULT DATA IS AVAILABLE  dataDefault*/
     /* if(isNA)
      {
        var loop=0
        while(loop!=eventLoop)
        {
          var totalOrderStack = parseInt(serialData.NA[loop].totalOrders);
          var totalShopStack =  parseInt(serialData.NA[loop].shoppingCartStarted);
          var sum = totalOrderStack/totalShopStack;
          var finalAnn = ((sum/OCRSUMS[loop])*ocrGlobal[loop]);
          dataNA.push(finalAnn);
          loop+=1;
        }
          var dataEnt ={};
          dataEnt.data =[];
          dataEnt.label = "NA";
          dataEnt.fillColor = "rgba(29, 34, 34,0.5)";
          dataEnt.strokeColor = "rgba(29, 34, 34,0.8)";
          dataEnt.highlightFill =  "rgba(29, 34, 34,0.75)";
          dataEnt.highlightStroke =  "rgba(29, 34, 34,1)";
          dataEnt.data =dataNA;
          datasets.push(dataEnt);
          log("Data Structure");
          log(datasets);
          log(dataEnt);
      }
*/




      if(isDEFAULT)
      {
         var loop=0
        while(loop!=eventLoop)
        {
          var totalOrderStack = parseInt(serialData.DEFAULT[loop].totalOrders);
          var totalShopStack =  parseInt(serialData.DEFAULT[loop].shoppingCartStarted);
          var sum = totalOrderStack/totalShopStack;
          var finalAnn = ((sum/OCRSUMS[loop])*ocrGlobal[loop]);
          dataDefault.push(finalAnn);
          loop+=1;
        }
          var dataEnt ={};
          dataEnt.data =[];
          dataEnt.label = "DEFAULT";
          dataEnt.fillColor = "rgba(165, 22, 236,0.5)";
          dataEnt.strokeColor = "rgba(165, 22, 236,0.8)";
          dataEnt.highlightFill =  "rgba(165, 22, 236,0.75)";
          dataEnt.highlightStroke =  "rgba(165, 22, 236,1)";
         dataEnt.data =dataDefault;
          datasets.push(dataEnt);
          log("Data Structure");
          log(datasets);
          log(dataEnt);
      }


       if(isEXISTINGCUSTOMER)
      {
         var loop=0
        while(loop!=eventLoop)
        {
          var totalOrderStack = parseInt(serialData.EXISTINGCUSTOMER[loop].totalOrders);
          var totalShopStack =  parseInt(serialData.EXISTINGCUSTOMER[loop].shoppingCartStarted);
          var sum = totalOrderStack/totalShopStack;
          var finalAnn = ((sum/OCRSUMS[loop])*ocrGlobal[loop]);
          dataExist.push(finalAnn);
          loop+=1;
        }
          var dataEnt ={};
          dataEnt.data =[];
          dataEnt.label = "EXISTINGCUSTOMER";
          dataEnt.fillColor = "rgba(19, 245, 108,0.5)";
          dataEnt.strokeColor = "rgba(19, 245, 108,0.8)";
          dataEnt.highlightFill =  "rgba(19, 245, 108,0.75)";
          dataEnt.highlightStroke =  "rgba(19, 245, 108,1)";
          dataEnt.data =dataExist;
          datasets.push(dataEnt);
          log("Data Structure");
          log(datasets);
          log(dataEnt);
      }


       if(isNEWCUSTOMER)
      {
         var loop=0
        while(loop!=eventLoop)
        {
          var totalOrderStack = parseInt(serialData.NEWCUSTOMER[loop].totalOrders);
          var totalShopStack =  parseInt(serialData.NEWCUSTOMER[loop].shoppingCartStarted);
          var sum = totalOrderStack/totalShopStack;
          var finalAnn = ((sum/OCRSUMS[loop])*ocrGlobal[loop]);
          dataNew.push(finalAnn);
          loop+=1;
        }
          var dataEnt ={};
          dataEnt.data =[];
          dataEnt.label = "NEWCUSTOMER";
          dataEnt.fillColor = "rgba(245, 19, 19,0.5)";
          dataEnt.strokeColor = "rgba(245, 19, 19,0.8)";
          dataEnt.highlightFill =  "rgba(245, 19, 19,0.75)";
          dataEnt.highlightStroke =  "rgba(245, 19, 19,1)";
          dataEnt.data =dataNew;
          datasets.push(dataEnt);
          log("Data Structure");
          log(datasets);
          log(dataEnt);
      }


       if(isEXISTINGCATALOGUECUSTOMER)
      {
         var loop=0
        while(loop!=eventLoop)
        {
          var totalOrderStack = parseInt(serialData.EXISTINGCATALOGUECUSTOMER[loop].totalOrders);
          var totalShopStack =  parseInt(serialData.EXISTINGCATALOGUECUSTOMER[loop].shoppingCartStarted);
          var sum = totalOrderStack/totalShopStack;
          var finalAnn = ((sum/OCRSUMS[loop])*ocrGlobal[loop]);
          dataExCatCust.push(finalAnn);
          loop+=1;
        }
          var dataEnt ={};
          data =[];
          dataEnt.label = "EXISTINGCATALOGUECUSTOMER";
          dataEnt.fillColor = "rgba(9, 102, 247,0.5)";
          dataEnt.strokeColor = "rgba(9, 102, 247,0.8)";
          dataEnt.highlightFill =  "rgba(9, 102, 247,0.75)";
          dataEnt.highlightStroke =  "rgba(9, 102, 247,1)";
          dataEnt.data =dataExCatCust;
          datasets.push(dataEnt);
          log("Data Structure");
          log(datasets);
          log(dataEnt);
      }


      chart.labels = labels;
      chart.datasets = datasets;

          log(chart);
          return chart;


    };






})(jQuery)