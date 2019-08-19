
app.controller('landingPageController', function($scope,$sce,$http,$rootScope,$q,$location,$filter,countryService,visitorService,periodService)
{

       // $scope.TR_CYCW = "-";
       // $scope.TD_CYCW = "-";
       // $scope.CA_CYCW = "-";
       // $scope.TV_CYCW = "-";
       // get selected country      
       <!--  

      //  $('#Visitor value').each(function() {
      //   var text = $(this).text();
      //   $(this).text(text.replace('NEW CUSTOMER','NEW VISITORS')); 
      // });


    // if( $('#Visitor option:contains("NEOMER")')){
    //   console.log("pppppppppppppppp");
    // }
    // else{console.log("xxxxxxxxxxxx");}
       // var nnn = $('select option:eq(1)').text();

        //console.log("pppppppppppppppp"+nnn);

        //var hoooo = "moooo";
      // $('#Visitor option:contains("NEW CUSTOMER")').remove();

       //$('#Visitor').html($('#Visitor').html().replace('NEW CUSTOMER','NEW VISITORS'));

        $rootScope.isActive = function (viewLocation) // active color change for li nav
        { 
            return viewLocation === $location.path();
        };
        $scope.foryear = new Date();

        $scope.backyear = new Date(); // making previous year using $filter in controller
        var myDate = new Date();  
        var previousYear = new Date(myDate);
        previousYear.setYear(myDate.getFullYear()-1);
        $scope.prevYear = $filter('date')(previousYear,'yyyy'); 


        $scope.countries = [{name: 'UNITED KINGDOM', id: 1 },{name: 'AUSTRIA', id: 2 },{ name: 'BELGIUM', id: 3},{name: 'BELGIUM(FL)',id: 4 },{name: 'ITALY',id: 5 },{ name: 'NETHERLANDS',id: 6},{ name: 'FRANCE', id: 7 },{name: 'IRELAND', id: 8},{name: 'SPAIN',id: 9 },{name: 'GERMANY',id: 10 },{name:'SWITZERLAND', id:11}, {name:'SWITZERLAND(FR)', id:12}];
        
        $scope.visitors = [{
          name: 'ALL VISITORS',
          val: 'ALL VISITORS',
          id: 1 // note the property casing here
        },{
          name: 'NEW VISITORS',
          val: 'NEW VISITORS',
          id: 2
        },        {
          name: 'EXISTING VISITORS',
          val: 'EXISTING VISITORS',
          id: 3
        }];

        $scope.lastweek = $.getlastWeek();



        $scope.weekrange = "Prior Week";

        $scope.sel_country = countryService.get();
        $scope.set = function(country) {
          countryService.set(country);
        };

        //$scope.euro = $sce.trustAsHtml('€');
        //$scope.pound_sign = "£";
        $scope.sel_visitor = visitorService.get();

        $scope.setV = function(visitor) {
          visitorService.set(visitor);
        };

        $scope.setP = function(weekrange){

          periodService.set(weekrange); 
          
        };
        $scope.content = periodService.get();

        var toWk = $.getCurrentWeek(); // actually last week
        var prevWk = $.getWeek(2); //last week
        var prev6Wk = $.getWeek(7); // last 6 week
        var prev13Wk = $.getWeek(14); // last 13 weeks
        var toWeek = services.toWeek + toWk;    // this is imporrtant *******************

        var toWk_Ly = $.getCurrentWeek_Ly(); // actually last week
        var prevWk_Ly = $.getWeek_Ly(2); //last week
        var prev6Wk_Ly = $.getWeek_Ly(7); // last 6 week
        var prev13Wk_Ly = $.getWeek_Ly(14); // last 13 weeks
        var toWeek_Ly = services.toWeek + toWk_Ly;  // this is imporrtant ****************


             
        $("#Visitor, #countries, #radio-div").change(function(){
          $scope.filterall();
        });
       

        // business indicators
        var clickStream_url = services.common1 + services.block1Weekly;
        //var sel_country = "UNITED KINGDOM";             
      

        $scope.filterall = function(){ 

          if($scope.content == "Prior Week"){
          $scope.weekRangeCY = 'Prior Week Current Year';
          $scope.weekRangeLY = 'Prior Week Last Year';
          };
          if($scope.content == "6 Weeks Avg"){
          $scope.weekRangeCY = 'Prior 6 Weeks Avg Current Year';
          $scope.weekRangeLY = 'Prior 6 Weeks Avg Last Year';
          };
          if($scope.content == "13 Weeks Avg"){
          $scope.weekRangeCY = 'Prior 13 Weeks Avg Current Year';
          $scope.weekRangeLY = 'Prior 13 Weeks Avg Last Year';
          };

          $scope.noClickstreamError = true;
          $scope.ClickstreamError = false;

          
          $scope.sel_country = countryService.get() || $scope.countries[0];
          $scope.sel_visitor = visitorService.get() || $scope.visitors[0];          
          
           var sel_visitor = $('#Visitor').find(":selected").text();   
           var sel_country = $('#countries').find(":selected").text() || "UNITED KINGDOM"; // For dashboard filed changes
           

          
          //$scope.symb = $sce.trustAsHtml('£');
          // if (sel_country ==  "AUSTRIA" || sel_country ==  "BELGIUM" || sel_country ==  "CZECH REPUBLIC" || sel_country ==  "ITALY" || sel_country ==  "NETHERLANDS" || sel_country ==  "POLAND" || sel_country ==  "IRELAND" || sel_country ==  "SLOVENIA" || sel_country ==  "GERMANY" || sel_country ==  "VIKING FRANCE" ){
          //   $scope.symb = $sce.trustAsHtml('€');
          // };
          if (sel_country ==  "UNITED KINGDOM" ){
            $scope.symb = $sce.trustAsHtml('£');
       
             }else{$scope.symb = $sce.trustAsHtml('€');

          };

          var fromWeek = services.fromWeek + toWk;   //1
          var fromWeek_Ly = services.fromWeek + toWk_Ly;  //2
          var from1Week = services.fromWeek + prevWk;      //3
          var toLWeek = services.toWeek + prevWk;

          var from1Week_Ly = services.fromWeek + prevWk_Ly;  //4
          var toLWeek_Ly = services.toWeek + prevWk_Ly;

          var from6Week = services.fromWeek + prev6Wk;  //5

          var from6Week_Ly = services.fromWeek + prev6Wk_Ly;      // 6
          var from13Week = services.fromWeek + prev13Wk;         // 7
          var from13Week_Ly = services.fromWeek + prev13Wk_Ly;  //8



          var funnel_url = services.common1 + services.block2nd;
          

             $http.get(funnel_url + fromWeek + toWeek, { cache: true})               //1
              .success(function(response) 
                   {                     

                 var graph = $.formatToJSON(response); 
                 if(graph == undefined){
                  $('.funnelhide0').css('display', 'block');
                 }
                 else{
                 var filteredCountries = $.filteredCountry(graph);
                 var filteredSegmentFunnel = $.filtered(filteredCountries); 
                 var funnelDataView = $.getStructuredDataFunnel(filteredSegmentFunnel);
                 $.funneDataChart(funnelDataView, '#funnelView1', "Last Week Current Year", 1);
                  }
            
                  });

              $http.get(funnel_url + fromWeek_Ly + toWeek_Ly, { cache: true})   // 2. CWPY  
              .success(function(response) 
                 { 

                 var graph = $.formatToJSON(response); 
                 if(graph == undefined){
                  $('.funnelhide1').css('display', 'block');
                 }
                 else{
                 var filteredCountries = $.filteredCountry(graph);
                 var filteredSegmentFunnel = $.filtered(filteredCountries); 
                 var funnelDataView = $.getStructuredDataFunnel(filteredSegmentFunnel);
                 $.funneDataChart(funnelDataView, '#funnelView2', "Last Week Last Year", 1);
                 }
                 
                 });



              $http.get(funnel_url + from1Week + toLWeek, { cache: true})           //3
              .success(function(response) 
                  {      
                 var graph = $.formatToJSON(response); 
                 var filteredCountries = $.filteredCountry(graph);
                 var filteredSegmentFunnel = $.filtered(filteredCountries); 
                 var funnelDataView = $.getStructuredDataFunnel(filteredSegmentFunnel);
                 $.funneDataChart(funnelDataView, '#funnelView3', "Prior Week Current year", 1);

                  });

              $http.get(funnel_url + from1Week_Ly + toLWeek_Ly, { cache: true})           //4
              .success(function(response) 
                  {      
                 var graph = $.formatToJSON(response); 
                 if(graph == undefined){
                  $('.funnelhide2').css('display', 'block');
                 }
                 else{
                 var filteredCountries = $.filteredCountry(graph);
                 var filteredSegmentFunnel = $.filtered(filteredCountries); 
                 var funnelDataView = $.getStructuredDataFunnel(filteredSegmentFunnel);
                 $.funneDataChart(funnelDataView, '#funnelView4', "Prior Week Last year", 1);
                  }
                  });

              $http.get(funnel_url + from6Week + toWeek, { cache: true})           //5
              .success(function(response) 
                  {      
                 var graph = $.formatToJSON(response); 
                 var filteredCountries = $.filteredCountry(graph);
                 var filteredSegmentFunnel = $.filtered(filteredCountries); 
                 var funnelDataView = $.getStructuredDataFunnel(filteredSegmentFunnel);
                 $.funneDataChart(funnelDataView, '#funnelView5', "Prior 6 Week Avg Current Year", 6);

                 });

              $http.get(funnel_url + from6Week_Ly + toWeek_Ly, { cache: true})           //6
              .success(function(response) 
                  {      
                 var graph = $.formatToJSON(response);
                 if(graph == undefined){
                  $('.funnelhide3').css('display', 'block');
                 }
                 else{ 
                 var filteredCountries = $.filteredCountry(graph);
                 var filteredSegmentFunnel = $.filtered(filteredCountries); 
                 var funnelDataView = $.getStructuredDataFunnel(filteredSegmentFunnel);
                 $.funneDataChart(funnelDataView, '#funnelView6', "Prior 6 Week Avg Last Year", 6);
                  }
                 });
               

              $http.get(funnel_url + from13Week + toWeek, { cache: true})           //7
              .success(function(response) 
                  {      
                 var graph = $.formatToJSON(response); 
                 var filteredCountries = $.filteredCountry(graph);
                 var filteredSegmentFunnel = $.filtered(filteredCountries); 
                 var funnelDataView = $.getStructuredDataFunnel(filteredSegmentFunnel);
                 $.funneDataChart(funnelDataView, '#funnelView7', "Prior 13 Week Avg Current Year", 13);

                 });

              $http.get(funnel_url + from13Week_Ly + toWeek_Ly, { cache: true})           //8
              .success(function(response) 
                  {      
                 var graph = $.formatToJSON(response); 
                 if(graph == undefined){
                  $('.funnelhide4').css('display', 'block');
                 }
                 else{
                 var filteredCountries = $.filteredCountry(graph);
                 var filteredSegmentFunnel = $.filtered(filteredCountries); 
                 var funnelDataView = $.getStructuredDataFunnel(filteredSegmentFunnel);
                 $.funneDataChart(funnelDataView, '#funnelView8', "Prior 13 Week Avg Last Year", 13);
                  }
                 });
               


 
               var headermain;
              var fromWeek = services.fromWeek + toWk;            // 1
              $http.get(clickStream_url + fromWeek + toWeek, { cache: true})
                  .success(function(response) 
                      {
                        var headermain=  $.tableDataDisplay(response);
                        //console.log("ooooooooooooooooooo" + headermain);
                        if(headermain == undefined){
                          $scope.noClickstreamError = false;
                          $scope.ClickstreamError = true;
                          $(".table.rt-align > tbody > tr > td").html("--");
                       }
                       else{
                        var filteredCountries = $.filteredCountry(headermain);
                        var returnedData = $.filtered(filteredCountries);
                        var mainData = $.getDatafortable(returnedData);
                        $scope.TR_CYCW = parseInt(mainData.TR);   
                        $scope.TO_CYCW_C = $filter('commaS')(parseInt(mainData.TO));
                        $scope.CA_CYCW_C = $filter('commaS')(parseInt(mainData.CA));
                        $scope.TV_CYCW_C = $filter('commaS')(parseInt(mainData.TV));
                        $scope.TO_CYCW = parseInt(mainData.TO);
                        $scope.CA_CYCW = parseInt(mainData.CA);
                        $scope.TV_CYCW = parseInt(mainData.TV);
                      }
                      })
                      .error(function(response){
                        //alert("Last Week not found");
                      });

                    // Last Week prebious year                              //2
              $http.get(clickStream_url + fromWeek_Ly + toWeek_Ly, { cache: true})
                  .success(function(response) 
                      {
                       var header=  $.tableDataDisplay(response);
                       if(header == undefined){
                        
                       }
                       else{
                        var filteredCountries = $.filteredCountry(header);
                        var returnedData = $.filtered(filteredCountries);
                        var mainData = $.getDatafortable(returnedData);
                        
                        $scope.TR_PYCW = parseInt(mainData.TR);   
                        $scope.TO_PYCW_C = $filter('commaS')(parseInt(mainData.TO));
                        $scope.CA_PYCW_C = $filter('commaS')(parseInt(mainData.CA));
                        $scope.TV_PYCW_C = $filter('commaS')(parseInt(mainData.TV));
                        $scope.TO_PYCW = parseInt(mainData.TO);
                        $scope.CA_PYCW = parseInt(mainData.CA);
                        $scope.TV_PYCW = parseInt(mainData.TV);
                      };
                      })
                      .error(function(response){
                        //alert("Last Week not found");
                      });

             
              $http.get(clickStream_url + from1Week + toLWeek, { cache: true})                  // 3 prior week calc
                  .success(function(response)   
                      {                        
                        var header=  $.tableDataDisplay(response);
                        
                        var filteredCountries = $.filteredCountry(header);
                        var returnedData = $.filtered(filteredCountries);
                        var mainData = $.getDatafortable(returnedData);
                         
                         $scope.TR_CYPW = parseInt(($scope.TR_CYCW - (mainData.TR) ) / (mainData.TR)*100)+" %" ;
                         $scope.TO_CYPW = parseInt(($scope.TO_CYCW - (mainData.TO) ) / (mainData.TO)*100)+" %" ;
                         $scope.CA_CYPW = parseInt(($scope.CA_CYCW - (mainData.CA) ) / (mainData.CA)*100)+" %" ;
                         $scope.TV_CYPW = parseInt(($scope.TV_CYCW - (mainData.TV) ) / (mainData.TV)*100)+" %" ;
                      })
                      .error(function(response){
                        //alert("previous week not found");
                      });

           
            $http.get(clickStream_url + from1Week_Ly + toLWeek_Ly, { cache: true})         // 4 prior week last year
                .success(function(response)   
                    {
                      var header=  $.tableDataDisplay(response);
                      
                        var filteredCountries = $.filteredCountry(header);
                        var returnedData = $.filtered(filteredCountries);
                        var mainData = $.getDatafortable(returnedData);
                       
                       $scope.TR_PYPW = parseInt(($scope.TR_PYCW - (mainData.TR) ) / (mainData.TR)*100)+" %" ;
                       $scope.TO_PYPW = parseInt(($scope.TO_PYCW - (mainData.TO) ) / (mainData.TO)*100)+" %" ;
                       $scope.CA_PYPW = parseInt(($scope.CA_PYCW - (mainData.CA) ) / (mainData.CA)*100)+" %" ;
                       $scope.TV_PYPW = parseInt(($scope.TV_PYCW - (mainData.TV) ) / (mainData.TV)*100)+" %" ;
                    })
                    .error(function(response){
                      //alert("previous week not found");
                    });

                      
              $http.get(clickStream_url + from6Week + toWeek, { cache: true})        //5 prior 6 week
                  .success(function(response)   
                      {
                        var header=  $.tableDataDisplay(response);
                        
                        var filteredCountries = $.filteredCountry(header);
                        var returnedData = $.filtered(filteredCountries);
                        var mainData = $.getDatafortable(returnedData);
                         $scope.TR_CY6W = parseInt(($scope.TR_CYCW - (mainData.TR / 6) ) / (mainData.TR / 6)*100)+" %";
                         $scope.TO_CY6W = parseInt(($scope.TO_CYCW - (mainData.TO / 6) ) / (mainData.TO / 6)*100)+" %";
                         $scope.CA_CY6W = parseInt(($scope.CA_CYCW - (mainData.CA / 6) ) / (mainData.CA / 6)*100)+" %";
                         $scope.TV_CY6W = parseInt(($scope.TV_CYCW - (mainData.TV / 6) ) / (mainData.TV / 6)*100)+" %";
                      })
                      .error(function(response){
                        //alert("previous week not found");
                      });


              
              $http.get(clickStream_url + from6Week_Ly + toWeek_Ly, { cache: true})          //6 prior 6 week last week
                  .success(function(response)   
                      {
                        var header=  $.tableDataDisplay(response);
                        
                        var filteredCountries = $.filteredCountry(header);
                        var returnedData = $.filtered(filteredCountries);
                        var mainData = $.getDatafortable(returnedData);
                         
                         $scope.TR_PY6W = parseInt(($scope.TR_PYCW - (mainData.TR / 6) ) / (mainData.TR / 6)*100)+" %";
                         $scope.TO_PY6W = parseInt(($scope.TO_PYCW - (mainData.TO / 6) ) / (mainData.TO / 6)*100)+" %";
                         $scope.CA_PY6W = parseInt(($scope.CA_PYCW - (mainData.CA / 6) ) / (mainData.CA / 6)*100)+" %";
                         $scope.TV_PY6W = parseInt(($scope.TV_PYCW - (mainData.TV / 6) ) / (mainData.TV / 6)*100)+" %";
                         
                      })
                      .error(function(response){
                        //alert("previous week not found");
                      });

              
              $http.get(clickStream_url + from13Week + toWeek, { cache: true})          //7 prior 13 week 
                  .success(function(response)   
                      {
                        var header=  $.tableDataDisplay(response);
                       
                        var filteredCountries = $.filteredCountry(header);
                        var returnedData = $.filtered(filteredCountries);
                        var mainData = $.getDatafortable(returnedData);
                         
                         $scope.TR_CY13W = parseInt(($scope.TR_CYCW - (mainData.TR / 13) ) / (mainData.TR / 13)*100)+" %";
                         $scope.TO_CY13W = parseInt(($scope.TO_CYCW - (mainData.TO / 13) ) / (mainData.TO / 13)*100)+" %";
                         $scope.CA_CY13W = parseInt(($scope.CA_CYCW - (mainData.CA / 13) ) / (mainData.CA / 13)*100)+" %";
                         $scope.TV_CY13W = parseInt(($scope.TV_CYCW - (mainData.TV / 13) ) / (mainData.TV / 13)*100)+" %";
                        
                      })
                      .error(function(response){
                        //alert("previous week not found");
                      });

                   
              $http.get(clickStream_url + from13Week_Ly + toWeek_Ly, { cache: true})   //8 prior 13 week last year
                  .success(function(response)   
                      {
                        var header=  $.tableDataDisplay(response);
                        
                        var filteredCountries = $.filteredCountry(header);
                        var returnedData = $.filtered(filteredCountries);
                        var mainData = $.getDatafortable(returnedData);
                         
                         $scope.TR_PY13W = parseInt(($scope.TR_PYCW - (mainData.TR / 13) ) / (mainData.TR / 13)*100)+" %";
                         $scope.TO_PY13W = parseInt(($scope.TO_PYCW - (mainData.TO / 13) ) / (mainData.TO / 13)*100)+" %";
                         $scope.CA_PY13W = parseInt(($scope.CA_PYCW - (mainData.CA / 13) ) / (mainData.CA / 13)*100)+" %";
                         $scope.TV_PY13W = parseInt(($scope.TV_PYCW - (mainData.TV / 13) ) / (mainData.TV / 13)*100)+" %";
                        
                      })
                      .error(function(response){
                        //alert("previous week not found");
                      });                     

                $scope.selectKpi = function(kpiurl,chartDiv,kpicalc){ //, chartDiv, kpiurl, kpicalc

                        $scope.finaldataList = [];
                       $('.overlayloading').css('display','block');
                         //$scope.selected = 'first';   // kpi page
                          
                          var kpiurlone = services.common1 + services.kpi1;  // first KPvar kpiurl1 = services.common1 + services.kpi1;I    
                          var kpiurl2 = services.common1 + services.kpi2; 
                          //console.log("kk" +  services.kpi1)

                       $http.get(kpiurl + fromWeek + toWeek, { cache: true})         //1       
                        .success(function(response) 
                             {  

                              var header=  $.formatToJSON(response);
                              if (header == undefined){
                                $scope.finaldataList.push({
                                        'label':'Last Week Current Year',
                                        value:0
                              });
                              }
                              else{
                             // console.log(JSON.stringify(header));
                              var filteredCountries = $.filteredCountry(header);
                              var returnedData = $.filtered(filteredCountries);
                              //console.log("LWCYYYY"+ JSON.stringify(returnedData));

                              var mainData = kpicalc(returnedData);

                              $scope.finaldataList.push({
                                        'label':'Last Week Current Year',
                                        value:mainData
                              })
                            }
                               $http.get(kpiurl + fromWeek_Ly + toWeek_Ly, { cache: true})   //2            
                              .success(function(response) 
                             {  
                              var header=  $.formatToJSON(response);
                              //console.log(JSON.stringify("pp" + header));
                              if (header == undefined){
                                $scope.finaldataList.push({
                                        'label':'Last Week Last Year',
                                        value:0
                              });
                              }
                              else{
                              var filteredCountries = $.filteredCountry(header);
                             
                              var returnedData = $.filtered(filteredCountries);
                              //console.log("LWLYYY"+ JSON.stringify(returnedData));
                              var mainData = kpicalc(returnedData);

                              $scope.finaldataList.push({
                                        'label':'Last Week Last Year',
                                        value:mainData
                              })
                             }

                                $http.get(kpiurl + from13Week + toWeek, { cache: true})          //7
                            .success(function(response)   
                                {
                              var header=  $.formatToJSON(response);
                              if (header == undefined){
                                $scope.finaldataList.push({
                                        'label':$scope.weekRangeCY,
                                        value:0
                              });
                              }
                              else{

                              var filteredCountries = $.filteredCountry(header);
                             
                              var returnedData = $.filtered(filteredCountries);

                              var durationWeek = $.filteredWeeks(returnedData);
                              //console.log("diff1"+ JSON.stringify(durationWeek));
                              var mainData = kpicalc(durationWeek);
                              $scope.finaldataList.push({
                                        'label':$scope.weekRangeCY,//'prior 13 Weeks Avg Week Current Year',
                                        value:mainData
                              })
                            }

                              $http.get(kpiurl + from13Week_Ly + toWeek_Ly, { cache: true})   //8
                              .success(function(response)   
                               {
                              var header=  $.formatToJSON(response);
                                  if (header == undefined){
                                      $scope.finaldataList.push({
                                        'label':$scope.weekRangeLY,
                                        value:0
                                    });
                                }
                                else{
                              var filteredCountries = $.filteredCountry(header);                             
                              var returnedData = $.filtered(filteredCountries);
                              var durationWeek = $.filteredWeeks(returnedData);
                              //console.log("diff2"+ JSON.stringify(durationWeek));
                              var mainData = kpicalc(durationWeek);                              
                              $scope.finaldataList.push({
                                        'label':$scope.weekRangeLY,//'prior 13 Weeks Avg Week Last Year',
                                        value:mainData
                              })
                              }
                               $('.overlayloading').css('display','none');
                                   $.discreteBarChartKpi(chartDiv, $scope.finaldataList);
                                  });   
                               });
                            }); 
                       });
                             
                        console.log("datametica" + $scope.finaldataList); 

                      };

                      
                      $scope.selectTab=function(dataid){// '#kpi3barChart', kpiurlonchartDive, kpicalc3  , kpiurl, kpicalc
                       //var dataid='first'
                       // $scope.selectTab(dataid,'#kpi1barChart')                       

                      if(dataid=='first'){
                          var kpiurl = services.common1 + services.kpi1;
                          var kpicalc=$.getDataforKPI1;
                          var chartspot = "#kpi1barChart";
                          $scope.kpiTitle = "ORDER CONVERSATION RATE";
                          $scope.selectKpi(kpiurl,chartspot,kpicalc);

                      }else if(dataid=='second'){
                          var kpiurl = services.common1 + services.kpi2;
                          var kpicalc=$.getDataforKPI2;
                          var chartspot = "#kpi2barChart";
                          $scope.kpiTitle = "AVG REVENUE PER ORDER";
                          $scope.selectKpi(kpiurl,chartspot,kpicalc);

                      }else if(dataid=='third'){
                          var kpiurl = services.common1 + services.kpi3;
                          var kpicalc=$.getDataforKPI3;
                          var chartspot = "#kpi3barChart";
                          $scope.kpiTitle = "AVG PAGE VIEWS PER VISIT";
                          $scope.selectKpi(kpiurl,chartspot,kpicalc);

                      }else if(dataid=='forthA'){
                          var kpiurl = services.common1 + services.kpi4a;
                          var kpicalc=$.getDataforKPI4a;
                          var chartspot = "#kpi4abarChart";
                          $scope.kpiTitle = "PERCENT REVENUE FROM NEW VISITORS";
                          $scope.selectKpi(kpiurl,chartspot,kpicalc);

                      }else if(dataid=='forthB'){
                          var kpiurl = services.common1 + services.kpi4b;
                          var kpicalc=$.getDataforKPI4b;
                          var chartspot = "#kpi4bbarChart";
                          $scope.kpiTitle = "PERCENT REVENUE FROM RETURNING VISITORS";
                          $scope.selectKpi(kpiurl,chartspot,kpicalc);

                      }else if(dataid=='fifth'){
                          var kpiurl = services.common1 + services.kpi5;
                          var kpicalc=$.getDataforKPI5;
                          var chartspot = "#kpi5barChart";
                          $scope.kpiTitle = "SHOPPING CART COMPLETION RATE";
                          $scope.selectKpi(kpiurl,chartspot,kpicalc);
                      }
                        $scope.selected = dataid;
                        $scope.selectedplace = chartspot;
                        
                    }

                  // var dataid = $scope.selected || "first";
                  // var chartid = '#kpi1barChart' || $scope.chartarea;
                  // $scope.selectTab(dataid,chartid);
                  var dataid = $scope.selected || "first";
                  var chartid =  $scope.selectedplace;
                  $scope.selectTab(dataid,chartid);
                  //$scope.selectTab(dataid,'#kpi1barChart')
               // var renderDatafirst='#kpi1barFinalChart'
               // var kpifunction=$.getDataforKPI1;
                   // $scope.firstORCChart(dataid,renderDatafirst,kpifunction);
                   // $scope.setP($scope.dataFilterData)                           

        };

       $scope.filterall();
});