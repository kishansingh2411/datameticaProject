



app.controller("mainCtrl",function($scope,$http,$rootScope)
{
  $rootScope.mapRegion ="European Region";

 /* $scope.orderByField = 'totalVisits';
   $scope.orderByFieldViewed = 'totalViewedProducts';
    $scope.orderByFieldCart = 'shoppingCartStarted';
     $scope.orderByFieldOrders = 'totalOrders';


  $scope.reverseSort = false;
    $scope.reverseSortorderByFieldViewed = false;
      $scope.reverseSortOrderByFieldCart = false;
        $scope.reverseSortOrderByFieldOrders = false;*/

         /*Modal PopUp*/
/*    $scope.showModal = false;
    $scope.toggleModal = function(){
        $scope.showModal = !$scope.showModal;
    };
*/
  
   $scope.sortType     = 'totalVisits'; // set the default sort type
   $scope.sortReverse  = false;  // set the default sort order
   $scope.searchFish   = '';     // set the default search/filter term

   log($.currentPage);
   $scope.defaultData ="Country";
   $scope.selectedCountry=0;
   $scope.displayCountry ={};
   $scope.tabView;

    $scope.getSelected = function()
   {
    log(this);
   //  $scope.defaultData =this.coun;
     log(this.coun);
     $scope.selectedCountry = this.coun;
   //   $scope.defaultData =this.coun;
    if($scope.selectedCountry=="ALL")
    {
        $scope.displayCountry = $scope.tabView;
    }
    else
    {
     var i=0;
     while(i!=$scope.tabView.length)
     { 
        log($scope.tabView.country);
        if($scope.selectedCountry== $scope.tabView[i].country)
        {
          $scope.displayCountry  = $scope.tabView[i];
          log($scope.displayCountry);
           log($scope.displayCountry.length);
        }
        i+=1;
    }
    log($scope.displayCountry.country);
  }


   };



   


  //Display Funnel View
/*      $.getDaysAgoNumber(365);
      number is number of days back 366 is 1 year back
      in YY-MM-DD Format

*/

        var current = $.getDaysAgoNumber(1);
        var previous =$.getDaysAgoNumber(366); 

      //  alert("Funnel View "+previous + " to "+current);
/*<<<<<<< HEAD
*/       

// var previous = parseInt(currentYear)+"-"+parseInt(currentMonth+2)+"-"+parseInt(currentDay-1);
/*=======
*/

/*>>>>>>> 6f627336d7faa3f47b88c5ace6d0a3a164bbfec3
*/   

        var toDate = services.dataTo +current;
        var fromDate = services.dataFrom + previous; 


        var val = $.diffDates(current,previous,0);
        log(val);
/*        $scope.DisplayCalenInfo = val.monthRef[0] + parseInt(currentDay-1)+ ","+parseInt(currentYear-1) + " - " + val.monthRef[0] + parseInt(currentDay-1)+ ","+parseInt(currentYear);
*/       


  var url = services.common + services.block2nd+fromDate+toDate;
  log(url);
  $http.get(url)
  .success(function(response) 
  {
     log(response);
      var graph = $.formatToJSON(response);   //Funnel View Data For Bar Chart
      log(graph);

    //polulate second Graph 
      //var formattedData  = $.structCommonJSONByCountry(graph);

      //Polulate DropDown With Country List
     //$scope.countryList = $.populateDropDownCountry('#dropdownMenuUL ul',graph);  //With Polulate DropDown Country Data
     // log( $scope.countryList);



      /*Display Country All In Index.html */

     // $scope.tabView = $.fillTabularView('#tableRowData',graph);  //With Polulate DropDown Country Data
     // log($scope.tabView);
     //  $scope.displayCountry = $scope.tabView;
     //  //Perform Calculation For Funnel View
     //  log(graph);
     //  var funnelDataView = $.getStructuredDataFunnel(graph);
     //  log(funnelDataView);
     //  log(funnelDataView.datasets[0].data);
     //  var percent= funnelDataView.datasets[0].data;

      /*Calculate Funnel View Percentile Based on User Visit*/
      $rootScope.shop =100+"%";
      $rootScope.order =  $.roundToTwo(Math.round(percent[1]/percent[0]*100))+"%";
      $rootScope.viewed = $.roundToTwo(Math.round(percent[2]/percent[0]*100))+"%";
      $rootScope.total =  $.roundToTwo(Math.round(percent[3]/percent[0]*100))+"%";


      /*Creating Test ChartJS*/
     // $.DrawChartsFunnel("funnelView2",funnelDataView);
    log("Funnel View Charts Data"); 
     log(graph);
       var dataHS = $.charts.funnelChartData(graph);

       log(dataHS);
       $.charts.drawFunnelChart(dataHS);

      /*Ends*/


      
        $scope.DisplayCalenInfo = $.getDaysAgo(366) + " To "+$.getDaysAgo(1);
      //  $scope.commonCalendar = val.monthRef[0] + " "+currentDay+ ","+parseInt(currentYear);

      });

});
  




