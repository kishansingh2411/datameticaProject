(function($)
{

  $.charts = $.charts || {};

  $.charts.funnelChartData = function(graph)
  {
  	
  	try
  	{

        var loop=0;
        var shoppingCartStartedSum=0;
        var totalOrdersSum=0;
        var totalViewedProductsSum=0;
        var totalVisitsSum=0;
        while(loop!=graph.dataSet.length)
        {
            shoppingCartStartedSum = shoppingCartStartedSum+parseInt(graph.dataSet[loop].shoppingCartStarted);
            totalOrdersSum  = totalOrdersSum+parseInt(graph.dataSet[loop].totalOrders);
            totalViewedProductsSum = totalViewedProductsSum+parseInt(graph.dataSet[loop].totalViewedProducts);
            totalVisitsSum = totalVisitsSum+parseInt(graph.dataSet[loop].totalVisits);
          	//log(loop);
          loop+=1;  
        }

        // log("Value Sum");
        // log(shoppingCartStartedSum);
        // log(totalOrdersSum);
        // log(totalViewedProductsSum);
        // log(totalVisitsSum);


  /*      var shopD  = parseInt(shoppingCartStartedSum/1000000);
        var orderSum = parseInt(totalOrdersSum/1000000);
        var viewedPro = parseInt(totalViewedProductsSum/1000000);
        var totalVisit = parseInt(totalVisitsSum/1000000);*/

        var shopD  = parseInt(shoppingCartStartedSum);
        var orderSum = parseInt(totalOrdersSum);
        var viewedPro = parseInt(totalViewedProductsSum);
        var totalVisit = parseInt(totalVisitsSum);


        var shop =100+"%";
      	var orderP =  $.roundToTwo(Math.round(viewedPro/totalVisit*100))+"%";
        var viewedP = $.roundToTwo(Math.round(shopD/totalVisit*100))+"%";
        var totalP =  $.roundToTwo(Math.round(orderSum/totalVisit*100))+"%";

		/*Create Chart For Funnel View*/
		var visited = "Visited 100%";
		var viewProd = "Viewed Products "+orderP;
		var PlacedCarts = "Placed Products In Cart "+viewedP;
		var placed = "Placed Order "+totalP;


		var tuple = {};
		var seriesPush =[];

		tuple.data =[];
		tuple.name = "Funnel View European Regions";
		//log(tuple);
		var param1 = [];
		var param2 = [];
		var param3 = [];
		var param4 = [];


		param1.push(visited);
		param1.push(totalVisit);
		//log(param1);


		param2.push(viewProd);
		param2.push(viewedPro);
		//log(param2);


		param3.push(PlacedCarts);
		param3.push(shopD);
		//log(param3);


		param4.push(placed);
		param4.push(orderSum);
		//log(param4);

		tuple.data.push(param1);
		tuple.data.push(param2);
		tuple.data.push(param3);
		tuple.data.push(param4);
		//log(tuple);

		seriesPush.push(tuple);
		//log(seriesPush);
		return seriesPush;
       

   }
   catch(e)
   {
    log("Error :");
    log(e);
   }
};



$.charts.drawFunnelChart = function(dataForFunnel, funnelplace, fntitle)
{
	 $(funnelplace).highcharts(
	 {
        chart: 
        {
            type: 'funnel',
            marginRight: 100
        },
        title: 
        {
            text: fntitle,
            x: -40
        },
        plotOptions: 
        {
            series: 
            {
                dataLabels: 
                {
                    enabled: true,
                    format: '<b>{point.name}</b> ({point.y:,.0f})',
                    color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black',
                    softConnector: false
                },
                neckWidth: '2%',
                neckHeight: '0%'

                //-- Other available options
                // height: pixels or percent
                // width: pixels or percent
            }
        },
         credits: {      // remove highchart.com link label from chart.
                 enabled: false
        },
        legend: 
        {
            enabled: false
        },
        series: dataForFunnel
    });










};


})(jQuery)

