/* chi2_ctrlr.js -- UI for building R data.frame from i2b2

Note: we assume Array.prototype has been extended with map, join,
since i2b2 uses the prototype framework.

TODO: localize jslint exceptions
*/
/*jslint maxlen: 79, nomen: true, white: true*/
/*jslint vars: true */
/*global $j, i2b2, Ajax */
/*global window, alert */
'use strict';
(function (exports, i2b2, tool_widgets, Ajax, $j) {
    var tw = tool_widgets;

    var DFTool = (function (_super) {
	tw.__extends(DFTool, _super);

	function DFTool(container, rgate, builder, chi2) {
            _super.apply(this, arguments);

	    this.concepts = [];

            this.pw = new tw.DropWidget(
		this, $j('#chi2-PRSDROP'), 0, tw.PRS);
	    $j('.username').text(i2b2.h.getUser());
            this.dirtyResultsData = true;
	}

	// http://stackoverflow.com/questions/710586/json-stringify-bizarreness
	if(window.Prototype) {
	    //delete Object.prototype.toJSON;
	    delete Array.prototype.toJSON;
	    //delete Hash.prototype.toJSON;
	    //delete String.prototype.toJSON;
	}

	DFTool.prototype.dropNotify = function (item, ix, kind) {
	    var that = this;
	    $j('#df_str').hide();
	    $j('#df_suggest').hide();
	    if (kind === this.pw.kind) {
                if (!this.prs || this.prs != item) {
                    this.dirtyResultsData = true;
                }
		this.prs = item;
		if (! $j("#filename").val()) {
		    $j("#filename").val("heron-" + this.pw.pset_id(this.prs));
		}
	    }
	    return true;
	};

	DFTool.prototype.params = function (choice){
	    var filename = $j('#filename').val();
            if(!this.prs) {
		return this.warn(
		    "Patient set missing; please drag one from previous queries.");
            }
	    // TODO: warn about uppercase? VL reported it doesn't work.
            /*
	    if (!/^[A-Za-z0-9\._\-]+$/.test(filename)) {
		return this.warn("Please use only letters, digits, period (.), underscore (_) or hyphen (-) in filenames.");
	    }
            */
	    //var backend = $j('#backend').val();

	    return {
		backend: 'chi2',
		r_script: 'dfbuilder.R',
		r_function: 'build.dataframes',
		patient_set: this.pw.pset_id(this.prs),
		label: this.pw.displayName(this.prs),
		concepts: '',
		filename: '',
                pgsize: exports.model.pgsize
	    };
	};

	DFTool.prototype.show_results = function (results) {
	    this.resultsElt.hide();
	    $j("#filename").val("");

            // Display results
            //$j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = results.str;
            var resp = $j.parseJSON(results.str);
            //$j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = resp.status;
            var tabstr = '<table border="1" border-collapse="collapse">';
            var r, c;
            tabstr += '\n<tr>';
            for (c=0; c < resp.cols.length; c++) {
                var data = resp.cols[c]; 
                if (c == 4) { data = $('chi2-colname').value.toUpperCase(); }
                else if (c == 5) { data = 'FRC_' + $('chi2-colname').value.toUpperCase(); }
                tabstr += '\n\t<th>' + data + '</th>';
            }
            tabstr += '\n</tr>';
            var foundMid = false;
            for (r=0; r < resp.rows.length; r++) {
                if (!foundMid && resp.rows[r][resp.cols.length-1] == -1
                && resp.rows[r][0] != 'TOTAL') {
                    foundMid = true;
                    tabstr += '\n<tr bgcolor="#c0c0c0"><td colspan=' + resp.cols.length + '>...</td></tr>';
                }
                tabstr += '\n<tr>';
                for (c=0; c < resp.cols.length; c++) {
                    var data =  resp.rows[r][c];
                    if (resp.rows[r][0] == 'TOTAL') {
                        if (c != 0 && c != 2 && c != 4) { data = ''; }
                    } else {
                        if (data == null) { data = ''; }
                        else if (c == 3 || c == 5) { data = data.toFixed(5); }
                        else if (c == 6) { data = data.toFixed(2); }
                    }
                    tabstr += '\n\t<td>' + data + '</td>';
                }
                tabstr += '\n</tr>';
            }
            tabstr += '\n</table>';
            $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = tabstr;

            // Load the concept category drop down
            //
            //var options = $("#concepts-select");
            //var categories = null; -- TODO: fill this
            //$.each(categories, function() {
            //    options.append($("<option />").val(this.value).text(this.text));
            //    });
           
            // Enable/disable widgets
            $j('#chi2-pgsize').attr('disabled', false);
            $j('#goButton').attr('disabled', true);
            //$j('#concepts-select').attr('disabled', false);   -- TODO
	};
	return DFTool;
    }(tw.RGateTool));

    function onConceptsLoad(concepts) {
        var resp = $j.parseJSON(results.str);
        $j('#concepts-select').append('<option value="fred">Fred</option>');
        $j('#concepts-select').val('fred');
    }

    exports.model = undefined;
    function Init(loadedDiv) {
	var rgate = tw.mkWebPostable('/cgi-bin/rgate.cgi', Ajax);
        //alert('chi here 1');
	var builder = tw.mkWebPostable('/cgi-bin/dfbuild.cgi', Ajax);
        //alert('chi here 2');
	var chi2 = tw.mkWebPostable('/cgi-bin/chi2_tabbed.cgi', Ajax);
        //alert('chi here 3');
	var dftool = new DFTool($j(loadedDiv), rgate, builder, chi2);
        //alert('chi here 4');
	exports.model = dftool;
        exports.model.pgsize = 10;

        // manage YUI tabs
        this.yuiTabs = new YAHOO.widget.TabView("chi2-TABS", {activeIndex:0});
        this.yuiTabs.on('activeTabChange', function(ev) { 
                //Tabs have changed 
                if (ev.newValue.get('id')=="chi2-TAB1") {
                        // user switched to Results tab
                        $('chi2-pgsize').value = exports.model.pgsize.toString();
                        if (exports.model.prs) {
                        // contact PDO only if we have data
                                if (exports.model.dirtyResultsData) {
                                        // recalculate the results only if the input data has changed
                                        pgGo();
                                }
                        }
                }
        });
        
        //alert('chi here 5');
        //$j('#concepts-select').attr('disabled', true);

        //alert('chi here 6');
        $j('#goButton').attr('disabled', true);
        $j('#goButton').click(function() {
            if (exports.model.pgsize != parseInt($('chi2-pgsize').value)) {
                pgGo();
            }
        });
        //alert('chi here 7');
        $j('#chi2-pgsize').attr('disabled', true);
        $j('#chi2-pgsize').keyup(function(e) {
            if (exports.model.pgsize != parseInt($('chi2-pgsize').value)) {
                $j('#goButton').attr('disabled', false);
            } else {
                $j('#goButton').attr('disabled', true);
            }
            if (e.which == 13) { $j('#goButton').click(); }  // Enter key
        });

        //alert('chi here 8');
    }
    exports.Init = Init;
    function Unload() {
	exports.model = undefined;
	return true;
    }
    exports.Unload = Unload;

    function pgGo() {
        var formSize = parseInt($('chi2-pgsize').value);
        if (!formSize || formSize < 1) {
            alert('View Results error: please enter a positive integer value for size');
            $('chi2-pgsize').value = exports.model.pgsize.toString();
            return;
        }
        exports.model.pgsize = formSize;
        $('chi2-pgsize').value = formSize;
        $j('#goButton').attr('disabled', true);
        $j('#chi2-pgsize').attr('disabled', true);
        //$j('#concepts-select').attr('disabled', true);
        //remove old results
        $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-directions")[0].hide();
        $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = '<div class="results-progress">Please wait while the chi2 results are loaded...</div><div class="results-progressIcon"></div>';
        $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-finished")[0].show();
        exports.model.dirtyResultsData = false;
        // give a brief pause for the GUI to catch up
        //setTimeout('exports.model.runTool();', 50);
        exports.model.runTool();
    }


}(i2b2.chi2, i2b2,
  i2b2.chi2_tool_widgets,
  Ajax, $j)
);
