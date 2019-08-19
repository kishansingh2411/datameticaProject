
    /*Format Data for Bar/Stack Chart based on Visitor Segment*/

    $.formatToStackChartDataAvgRpo  = function(serialData)
    {

      /*var totalOrderStack = parseInt(serialData.EXISTINGCUSTOMER[loop].totalOrders);
      var totalRevenueStack =  parseInt(serialData.EXISTINGCUSTOMER[loop].totalRevenue);*/
      log("Data To Be Compiled For Chart AVGRPO");
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

        if(serialData.NA.length!=0)
        {
          log("NA >0 "+serialData.NA.length);
          isNA =true;
          eventLoop = serialData.NA.length;
          timeStampData = serialData.NA;
        }
        else
        {
          log("NA LESS OR EQUAL TO 0");
        }

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

        if(isNA)
        {  
          if(loop!=serialData.NA.length&&loop<=serialData.NA.length)
          ocrNA = parseFloat(serialData.NA[loop].totalOrders/serialData.NA[loop].totalRevenue);
        }


        if(isDEFAULT)
        {  
          ocrDef = parseFloat(serialData.DEFAULT[loop].totalRevenue/serialData.DEFAULT[loop].totalOrders);
        }
        if(isEXISTINGCATALOGUECUSTOMER)
        {  
          ocrExCatCust = parseFloat(serialData.EXISTINGCATALOGUECUSTOMER[loop].totalRevenue/serialData.EXISTINGCATALOGUECUSTOMER[loop].totalOrders);
        }

        if(isNEWCUSTOMER)
        {  
         ocrNew  = parseFloat(serialData.NEWCUSTOMER[loop].totalRevenue/serialData.NEWCUSTOMER[loop].totalOrders);
        }
        if(isEXISTINGCUSTOMER)
        {  
         ocrExt  = parseFloat(serialData.EXISTINGCUSTOMER[loop].totalRevenue/serialData.EXISTINGCUSTOMER[loop].totalOrders);
        }
        OCRSUMS.push(ocrExCatCust+ocrNew+ocrExt+ocrDef+ocrNA);
        loop+=1;
      }


      /*IF DEFAULT DATA IS AVAILABLE  dataDefault*/
      if(isNA)
      {
        var loop=0
        while(loop!=serialData.NA.length)
        {
          var totalOrderStack = parseInt(serialData.NA[loop].totalOrders);
          var totalVisitsStack =  parseInt(serialData.NA[loop].totalRevenue);
          var sum = totalOrderStack/totalVisitsStack;
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





      if(isDEFAULT)
      {
         var loop=0
        while(loop!=serialData.DEFAULT.length)
        {
          var totalOrderStack = parseInt(serialData.DEFAULT[loop].totalOrders);
          var totalRevenueStack =  parseInt(serialData.DEFAULT[loop].totalRevenue);
          var sum = totalRevenueStack/totalOrderStack;
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
        while(loop!=serialData.EXISTINGCUSTOMER.length)
        {
          var totalOrderStack = parseInt(serialData.EXISTINGCUSTOMER[loop].totalOrders);
          var totalRevenueStack =  parseInt(serialData.EXISTINGCUSTOMER[loop].totalRevenue);
          var sum = totalRevenueStack/totalOrderStack;
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
        while(loop!=serialData.NEWCUSTOMER.length)
        {
          var totalOrderStack = parseInt(serialData.NEWCUSTOMER[loop].totalOrders);
          var totalRevenueStack =  parseInt(serialData.NEWCUSTOMER[loop].totalRevenue);
          var sum = totalRevenueStack/totalOrderStack;
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
        while(loop!=serialData.EXISTINGCATALOGUECUSTOMER.length)
        {
          var totalOrderStack = parseInt(serialData.EXISTINGCATALOGUECUSTOMER[loop].totalOrders);
          var totalRevenueStack =  parseInt(serialData.EXISTINGCATALOGUECUSTOMER[loop].totalRevenue);
          var sum = totalRevenueStack/totalOrderStack;
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
