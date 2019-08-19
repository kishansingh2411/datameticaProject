(function($)
	{

	var recognition;
	var recognizing = false;
	$.init = function($location)
	{
		// new instance of speech recognition
    var recognition = new webkitSpeechRecognition();
    // set params
    recognition.continuous = true;
    recognition.interimResults = true;
    recognition.start();



    recognition.onresult = function(event)
    {
      
      // delve into words detected results & get the latest
      // total results detected
      var resultsLength = event.results.length -1 ;
      // get length of latest results
      var ArrayLength = event.results[resultsLength].length -1;
      // get last word detected
      var saidWord = event.results[resultsLength][ArrayLength].transcript;
      console.log(saidWord + " "+saidWord.length);
        if(saidWord=="average revenue per order"||saidWord=="average revenue"||saidWord=="per order")
        {
          
          window.location.replace(location.pathname+"#/avgRevenue");
          console.log("Opening ARPO");
        }
        else if(saidWord=="ocr")
        {
          
          window.location.replace(location.pathname+"#/charts");
          console.log("Opening OCR");
        } 
        else if(saidWord=="average page"||saidWord=="average page view")
        {
          
          window.location.replace(location.pathname+"#/avgPageViews");
          console.log("Opening AVGPPV");
        } 
        else if(saidWord=="new visitors"||saidWord=="percent revenue")
        {
          
          window.location.replace(location.pathname+"#/avgRevNewVistor");
          console.log("Opening NEW VISITORS");
        } 

         else if(saidWord=="exist visitors"||saidWord=="percent revenue")
        {
          
          window.location.replace(location.pathname+"#/avgRevExistVisitors");
          console.log("Opening EXISITING VISITORS");
        } 
        else if(saidWord=="shopping cart"||saidWord=="completion")
        {
          window.location.replace(location.pathname+"#/shoppingCartCompletion");
          console.log("Opening SHOPPING CART");
        } 


      
    
      // append the last word to the bottom sentence
      log("Said WORD "+ saidWord+ " Length : "+saidWord.length);
    }

    // speech error handling
    recognition.onerror = function(event)
    {
      console.log('error?');
      console.log(event);
    }
      		 
		    




  };

  		
		
	})(jQuery);




  //console.log(final_transcript);
        