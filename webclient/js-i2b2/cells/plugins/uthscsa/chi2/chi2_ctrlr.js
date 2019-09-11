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
            if (exports.model.toCsv) {
                return {
                    backend: 'chi2',
                    patient_set_1: pset1,
                    patient_set_2: this.pw2.pset_id(this.prs2),
                    pgsize: 'ALL',
                    cutoff: 1,
                    concepts: 'ALL',
                    extant: 1
                };
            }
            else {
                return {
                    backend: 'chi2',
                    patient_set_1: pset1,
                    patient_set_2: this.pw2.pset_id(this.prs2),
                    pgsize: exports.model.pgsize,
                    cutoff: exports.model.cutoff,
                    concepts: exports.model.concepts,
                    extant: exports.model.extant
                };
            }
	};

	DFTool.prototype.show_error = function (responseText) {
            $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = responseText;
            exports.model.extant = 0;
        };

	DFTool.prototype.show_504 = function () {
            exports.model.extant = 1;
            exports.model.runTool();
        };

	DFTool.prototype.show_results = function (results) {
	    this.resultsElt.hide();
            exports.model.extant = 0;

            // Parse results data
            var resp = $j.parseJSON(results.str);
            //alert(resp.status);

            // Use UI defined column names
            var p1name = $('chi2-p1-colname').value.toUpperCase().replace(/\W+/g, '_');
            var p2name = $('chi2-p2-colname').value.toUpperCase().replace(/\W+/g, '_');
            resp.cols[3] = p1name;
            resp.cols[4] = 'FRC_' + p1name;
            resp.cols[5] = p2name;
            resp.cols[6] = 'FRC_' + p2name;

            if (resp.rows.length == 0) { 
                if (resp.status.startsWith("No data for PSID")) { 
                    DFTool.prototype.show_504();
                    return; 
                }

                // No results, display status message
                $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = resp.status;
            }
            else if (exports.model.toCsv) { 
                // Export results to csv file via data URI
                var csvOut = resp.cols.join(',') + '\n';
                resp.rows.forEach(function(rdata, ri) {
                    var row = [];
                    resp.cols.forEach(function(cdata, ci) {
                        if (ci == 0 || ci == 1) {
                            // Wrap PREFIX, CCD in quotes
                            row.push('"' + resp.rows[ri][ci] + '"');
                        }
                        else if (resp.rows[ri][ci] != null && ci == 2) {
                            // Wrap concept NAME in quotes, handle embedded quotes
                            var rstr = resp.rows[ri][ci].replace(/\"/g, '""');
                            row.push('"' + rstr + '"');
                        }
                        else {
                            row.push(resp.rows[ri][ci]);
                        }
                    });
                    var str = row.join(',');
                    csvOut += ri < resp.rows.length ? str + '\n' : str;
                });
                // Create data URI
                $j('#exportLink').attr('href', 
                  'data:text/csv;charset=utf-8,' + encodeURIComponent(csvOut));
                var filename = p1name + '_' +  p2name + '.csv';
                $j('#exportLink').attr('download', filename);
                //$j('#exportLink').click();  // doesn't work, use plain Javascript
                document.getElementById("exportLink").click();
                // Restore previously displayed data
                $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = 
                    exports.model.saveHTML;
                exports.model.toCsv = false;
            }
            else {
                var tabstr = '<table id="chi2-result-tbl" border="1" border-collapse="collapse">';
                var r, c, p;
                tabstr += '\n<tr>';
                // Header row (skip PREFIX, start at index==1)
                for (c=1; c < resp.cols.length; c++) {
                    tabstr += '\n\t<th>' + resp.cols[c] + '</th>';
                }
                tabstr += '\n</tr>';
                var foundMid = false;
                for (r=0; r < resp.rows.length; r++) {
                    if (!foundMid && resp.rows[r][resp.cols.length-1] == -1
                    && resp.rows[r][0] != 'TOTAL') {
                        // Empty/gray row between positive/negative values
                        foundMid = true;
                        tabstr += '\n<tr bgcolor="#c0c0c0"><td colspan=' + resp.cols.length + '>...</td></tr>';
                    }
                    tabstr += '\n<tr>';
                    // Data row (skip PREFIX, start at index==1)
                    for (c=1; c < resp.cols.length; c++) {
                        var data =  resp.rows[r][c];
                        if (resp.rows[r][0] == 'TOTAL') {
                            // TOTAL row should only show CCD and counts
                            if (c != 1 && c != 3 && c != 5) { data = ''; }
                        } else {
                            if (data == null) { data = ''; }
                            // Frequencies rounded to 5 decimal places
                            else if ((c == 4 || c == 6) && !isNaN(data)) { data = data.toFixed(5); }
                            // Chi-squared rounded to 2 decimal places
                            else if (c == 7 && !isNaN(data)) { data = data.toFixed(2); }
                        }
                        tabstr += '\n\t<td>' + data + '</td>';
                    }
                    tabstr += '\n</tr>';
                }
                tabstr += '\n</table>';
                $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = tabstr;

                // Load the concept category drop down, if not already loaded
                if ($j('#concepts-select option').length == 1) {
                    for (p=0; p < resp.prefixes.length; p++) {
                        var code = resp.prefixes[p][0];
			// to avoid prefix ambiguities
			if (code.substr(-1) != ':') { code += ':' };
                        var desc = resp.prefixes[p][1];
                        $j('#concepts-select').append('<option value="' + code + '">' + desc + '</option>');
                    }
                }
            }
            // Enable/disable widgets
            enableWidgets(false);
            $j('#goButton').attr('disabled', true);
	};
	return DFTool;
    }(tw.RGateTool));


    exports.model = undefined;
    function Init(loadedDiv) {
	var chi2 = tw.mkWebPostable('/cgi-bin/chi2.cgi', Ajax);
        //alert('chi here 3');
	var dftool = new DFTool($j(loadedDiv), chi2);
        //alert('chi here 4');
	exports.model = dftool;
        exports.model.pgsize = 10;
        exports.model.cutoff = 10;
        exports.model.concepts = 'ALL';
        exports.model.saveHTML = '';
        exports.model.extant = 0;

        // manage YUI tabs
        this.yuiTabs = new YAHOO.widget.TabView("chi2-TABS", {activeIndex:0});
        this.yuiTabs.on('activeTabChange', function(ev) { 
            //Tabs have changed 
            if (ev.newValue.get('id')=="chi2-TAB1") {
                // user switched to Results tab
                $('chi2-pgsize').value = exports.model.pgsize.toString();
                $('chi2-cutoff').value = exports.model.cutoff.toString();
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
                            var c1 = $('chi2-p1-colname').value.toUpperCase().replace(/\W+/g, '_') + '_';
                            var c2 = $('chi2-p2-colname').value.toUpperCase().replace(/\W+/g, '_') + '_';
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
        $j("#concepts-select").val('ALL');
        $j('#concepts-select').attr('disabled', true);
        $j("#concepts-select").change(function(){
            if (exports.model.concepts != $j("#concepts-select").val()) {
                $j('#goButton').attr('disabled', false);
            }
            else if (exports.model.pgsize == parseInt($('chi2-pgsize').value)
            && exports.model.cutoff == parseInt($('chi2-cutoff').value)) {
                $j('#goButton').attr('disabled', true);
            }
        });
        //alert('chi here 6.1');
        $j('#exportButton').attr('disabled', true);
        $j('#exportButton').click(function() {
            exports.model.toCsv = true;
            pgGo();
        });
        //alert('chi here 7');
        $j('#goButton').attr('disabled', true);
        $j('#goButton').click(function() {
            if (exports.model.pgsize != parseInt($('chi2-pgsize').value)
            || exports.model.cutoff != parseInt($('chi2-cutoff').value)
            || exports.model.concepts != $j("#concepts-select").val()) {
                pgGo();
            }
        });
        //alert('chi here 8');
        $j('#chi2-pgsize').attr('disabled', true);
        $j('#chi2-pgsize').keyup(function(e) {
            if (exports.model.pgsize != parseInt($('chi2-pgsize').value)) {
                $j('#goButton').attr('disabled', false);
            }
            else if (exports.model.concepts == $j("#concepts-select").val()
            && exports.model.cutoff == parseInt($('chi2-cutoff').value)) {
                $j('#goButton').attr('disabled', true);
            }
            if (e.which == 13) { $j('#goButton').click(); }  // Enter key
        });
        //alert('chi here 9');
        $j('#chi2-cutoff').attr('disabled', true);
        $j('#chi2-cutoff').keyup(function(e) {
            if (exports.model.cutoff != parseInt($('chi2-cutoff').value)) {
                $j('#goButton').attr('disabled', false);
            }
            else if (exports.model.concepts == $j("#concepts-select").val()
            && exports.model.pgsize == parseInt($('chi2-pgsize').value)) {
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

    function enableWidgets(disabled) {
        $j('#exportButton').attr('disabled', disabled);
        $j('#goButton').attr('disabled', disabled);
        $j('#chi2-pgsize').attr('disabled', disabled);
        $j('#chi2-cutoff').attr('disabled', disabled);
        $j('#concepts-select').attr('disabled', disabled);
    }
    

    function pgGo() {
        enableWidgets(true);
        if (exports.model.toCsv) {
            $('chi2-pgsize').value = exports.model.pgsize.toString();
            $('chi2-cutoff').value = exports.model.cutoff.toString();
            $j("#concepts-select").val(exports.model.concepts);
            exports.model.saveHTML = $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML; 
            $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = '<div class="results-progress">Exporting data, please wait...</div><div class="results-progressIcon"></div>';
            exports.model.runTool();
            return; // no UI update for export to CSV
        } 
        var formSize = parseInt($('chi2-pgsize').value);
        if (!formSize || formSize < 1) {
            alert('View Results error: please enter a positive integer value for size');
            $('chi2-pgsize').value = exports.model.pgsize.toString();
            return;
        }
        exports.model.pgsize = formSize;
        var cutoff = parseInt($('chi2-cutoff').value);
        if (!cutoff || cutoff < 1) {
            alert('View Results error: please enter a positive integer value for cutoff');
            $('chi2-cutoff').value = exports.model.cutoff.toString();
            return;
        }
        exports.model.cutoff = cutoff;
        exports.model.concepts = $j("#concepts-select").val();
        $('chi2-pgsize').value = formSize;
        $('chi2-cutoff').value = cutoff;
        $j('#chi2-stats').text('');
        //remove old results
        $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-directions")[0].hide();
        $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-chi2")[0].innerHTML = '<div class="results-progress"><div id="refWork2QS" style="display: inline;"><img width="16" border="0" height="16" title="Refresh Workplace" alt="Refresh Workplace" src="assets/images/spin.gif">Please wait while the chi2 results are loaded...</div></div>';

        $j("DIV#analysis-mainDiv DIV#chi2-TABS DIV.results-finished")[0].show();
        exports.model.dirtyResultsData = false;
        exports.model.runTool();
    }


}(i2b2.chi2, i2b2,
  i2b2.chi2_tool_widgets,
  Ajax, $j)
);
