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

	function DFTool(container, chi2) {
            _super.apply(this, arguments);

            this.pw1 = new tw.DropWidget(
		this, $j('#chi2-p1-PRSDROP'), 0, tw.PRS);

            this.pw2 = new tw.DropWidget(
		this, $j('#chi2-p2-PRSDROP'), 1, tw.PRS);

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
	    $j('#df_suggest').hide();
	    if (kind === this.pw1.kind && ix === this.pw1.ix) {
		this.prs1 = item;
                this.dirtyResultsData = true;
                $j('#chi2-stats').text('');
                $j('#chi2-p1-colname').val('REF_' + this.pw1.pset_id(item));
	    }
	    if (kind === this.pw2.kind && ix === this.pw2.ix) {
		this.prs2 = item;
                this.dirtyResultsData = true;
                $j('#chi2-stats').text('');
                $j('#chi2-p2-colname').val('TEST_' + this.pw2.pset_id(item));
            }
	    return true;
	};

	DFTool.prototype.params = function (choice){
            var pset1 = null;
            if (!this.prs1) {
                pset1 = 0;
            }
            else {
                pset1 = this.pw1.pset_id(this.prs1);
            }
	    return {
		backend: 'chi2',
		patient_set_1: pset1,
		patient_set_2: this.pw2.pset_id(this.prs2),
                pgsize: exports.model.pgsize
	    };
	};

	DFTool.prototype.show_results = function (results) {
	    this.resultsElt.hide();

            // Display results
            var resp = $j.parseJSON(results.str);
            if (resp.rows.length == 0) { 
                $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = resp.status;
            }
            else {
                var tabstr = '<table id="chi2-result-tbl" border="1" border-collapse="collapse">';
                var r, c;
                tabstr += '\n<tr>';
                for (c=0; c < resp.cols.length; c++) {
                    var data = resp.cols[c]; 
                    if (c == 2) { data = $('chi2-p1-colname').value.toUpperCase(); }
                    else if (c == 3) { data = 'FRC_' + $('chi2-p1-colname').value.toUpperCase(); }
                    else if (c == 4) { data = $('chi2-p2-colname').value.toUpperCase(); }
                    else if (c == 5) { data = 'FRC_' + $('chi2-p2-colname').value.toUpperCase(); }
                    tabstr += '\n\t<th>' + data + '</th>';
                }
                tabstr += '\n</tr>';
                var foundMid = false;
                var stddev = null;
                var variance = null;
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
                            if (c == 6) { stddev = data; }
                            if (c == 7) { variance = data; }
                            if (c != 0 && c != 2 && c != 4) { data = ''; }
                        } else {
                            if (data == null) { data = ''; }
                            else if ((c == 3 || c == 5) && !isNaN(data)) { data = data.toFixed(5); }
                            else if (c == 6 && !isNaN(data)) { data = data.toFixed(2); }
                        }
                        tabstr += '\n\t<td>' + data + '</td>';
                    }
                    tabstr += '\n</tr>';
                }
                tabstr += '\n</table>';
                $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = tabstr;
                if (isNaN(stddev) || isNaN(variance)) { $j('#chi2-stats').text(''); }
                else { $j('#chi2-stats').text('stdev.p=' + stddev.toFixed(5) + ', var.p=' + variance.toFixed(5)); }
                // Load the concept category drop down
                //  TODO
               
                // Enable/disable widgets
                $j('#chi2-pgsize').attr('disabled', false);
                $j('#goButton').attr('disabled', true);
            }
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
	var chi2 = tw.mkWebPostable('/cgi-bin/chi2.cgi', Ajax);
        //alert('chi here 3');
	var dftool = new DFTool($j(loadedDiv), chi2);
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
                //if (exports.model.prs1 && exports.model.prs2) {
                if (exports.model.prs2) {
                    // contact PDO only if we have data
                    if (exports.model.dirtyResultsData) {
                        // recalculate the results only if the input data has changed
                        pgGo();
                    }
                    else {
                        // allow user to reset column names in browser
                        var tabstr = $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML;
                        if (tabstr.startsWith('<table id="chi2-result-tbl"')) {
                            var c1 = $j('#chi2-p1-colname').val().toUpperCase();
                            var c2 = $j('#chi2-p2-colname').val().toUpperCase();
                            var tab = document.getElementById('chi2-result-tbl');
                            tab.rows[0].cells[2].innerHTML = c1;
                            tab.rows[0].cells[3].innerHTML = 'FRC_' + c1;
                            tab.rows[0].cells[4].innerHTML = c2;
                            tab.rows[0].cells[5].innerHTML = 'FRC_' + c2;
                        }
                    }
                }
            }
        });
        
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
        $j('#chi2-stats').text('');
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
