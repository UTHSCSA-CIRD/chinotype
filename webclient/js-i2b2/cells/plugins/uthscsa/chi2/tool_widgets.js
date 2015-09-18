/* module i2b2.tool_widgets

TODO: isolate this module so it doesn't interact
      with other plugins
todo: consider eliminating jQuery dependency
 */
/*jslint maxlen: 79, nomen: true, white: true*/
/* TODO: consider getting rid of alert() */
/*global $j, i2b2, alert */
'use strict';

(function (exports, i2b2, $j, alert) {
    var __extends, DropWidget, PatientSetWidget, ConceptWidget, RGateTool;

    // an idiom from TypeScript's class definition.
    var __extends = function (d, b) {
        function Thing() { this.constructor = d; }
        Thing.prototype = b.prototype;
        d.prototype = new Thing();
    };
    exports.__extends = __extends;

    DropWidget = (function () {
	// TODO: document params
        function DropWidget(sink, container, ix, kind) {
            this.sink = sink;
            this.container = container;
            this.ix = ix;
	    this.kind = kind;
	    this.attach();
        }

        DropWidget.prototype.attach = function () {
            var that = this,
                k = this.kind,
                cid = this.container.attr('id'),
                op_trgt = { dropTarget: true },
                dropped = function (data) {
                    that.dropped(data);
		};

            i2b2.sdx.Master.AttachType(cid, k, op_trgt);
            i2b2.sdx.Master.setHandlerCustom(cid, k, "DropHandler", dropped);
        };

        // change "blah [2-1-2012]..." to just "blah " 
        DropWidget.prototype.cleanLabel = function () {
            return this.container.text().split('[')[0];
        };

        DropWidget.prototype.dropped = function (sdxData) {
            var item = sdxData[0],
                container = this.container;
            container.text(this.displayName(item));
            container.css("background", "#CFB");
            setTimeout(function () {
                container.css("background", '#DEEBEF');
            }, 250);
            this.sink.dropNotify(item, this.ix, this.kind);
        };

	DropWidget.prototype.displayName = function(dropInfo) {
            return dropInfo.sdxInfo.sdxDisplayName;
	};
	DropWidget.prototype.concept_key = function(dropInfo) {
	    //assert dropInfo.type === QM
            return dropInfo.sdxInfo.sdxKeyValue;
	};
	DropWidget.prototype.pset_id = function(dropInfo) {
            return dropInfo.sdxInfo.sdxKeyValue;
	};
	DropWidget.prototype.qm_id = function(dropInfo) {
            return dropInfo.sdxInfo.sdxKeyValue;
	};

	DropWidget.prototype.mkItem = function(n, k) {
	    // Let's hope this is enough...
	    return {sdxInfo: { sdxDisplayName: n,
			       sdxKeyValue: k,
			     },
		    kind: this.kind
		   };
	};

        return DropWidget;
    }());
    exports.DropWidget = DropWidget;

    var PRS = 'PRS', CONCPT = 'CONCPT', QM = 'QM';
    exports.PRS = PRS;
    exports.CONCPT = CONCPT;
    exports.QM = QM;

    function mkWebPostable(url, Ajax) {
        //alert("mkWebPostable url:\n" + url);
        return {
            post: function (params, onSuccess, onFailure) {
                return new Ajax.Request(url, {
                    method: 'post',
                    parameters: params,
                    evalJSON: true,
		    onSuccess: onSuccess,
		    onFailure: onFailure
                });
            }
        };
    }
    exports.mkWebPostable = mkWebPostable;

    // vestige of sharing code with KMStat plug-in
    // TODO: verify that getting rid of this doesn't
    //       interfere with KMStat
    exports.plot_output_path = "plot_output/";

    RGateTool = (function () {

        function RGateTool(container, rgate, builder, chi2) {
            this.container = container;
	    this.rgate = rgate;
	    this.builder = builder;
	    this.chi2 = chi2;
            var that = this;
            // in theory, these are: container.select('#info') etc.
            this.infoElt = $j('#info');
            this.resultsElt = $j('#rgate_results');

        }

        RGateTool.prototype.runTool = function() {
	    var choice = function (n) {
		return $j('input[name="' + n + '"]:checked').val();
	    };

	    var params = this.params(choice);
            if (params) {
		params.username = i2b2.h.getUser();
		params.password = i2b2.h.getPass();
                this.runAnalysisAndGetResults(params);
            }
        };

        RGateTool.prototype.warn = function (txt) {
            this.infoElt.html("<strong>" + txt + "</strong>");
            return false;
        };

        // TODO: make icefish path configurable
        exports.animation_addr = (
            "js-i2b2/cells/plugins/fsm/KaplanMeierStat/assets/icefish2.gif");
        RGateTool.prototype.startFishing = function () {
            this.resultsElt.html("<br/><img src='"
                                 + exports.animation_addr
                                 + "' />");
	    this.resultsElt.show();
            this.infoElt.html("");
        };

	// ode to python...
	var pop = function(o, k) {
	    var v = o[k];
	    delete o[k];
	    return v;
	};

        RGateTool.prototype.runAnalysisAndGetResults = function (params) {
            var that = this,
                show_results = function (xhr) {
                    that.show_results(xhr.responseJSON);
		},
                show_error = function (xhr) {
                    that.resultsElt.html("<br />");
                    alert("error from back-end:\n" + xhr.responseText);
		};

            this.startFishing();

            var backend = pop(params, 'backend') == 'builder' ? this.builder : this.rgate;
            backend = this.chi2;
            backend.post(params, show_results, show_error);
            
        };

       // vestige of sharing code with KMStat plug-in
       // TODO: verify that getting rid of this doesn't
       //       interfere with KMStat
	RGateTool.prototype.loadImg = function (path) {
            this.resultsElt.html(
		"<br/>"
                    + "<div><img src='" + path
                    + "' width='500' height='500' /></div>"
                    + "<br/>"
                    + "<a TARGET='_blank' href='" + path
                    + "'>View Kaplan Meier Survival Curves "
                    + "full size in a new window</a>");
	};
	return RGateTool;
    }());
    exports.RGateTool = RGateTool;

    function getQueryDef(ajax, qm_id, then) {
	ajax.getRequestXml_fromQueryMasterId("CRC:QueryTool",
					     { qm_key_value: qm_id },
					     then);
    }
    exports.getQueryDef = getQueryDef;

}(i2b2.chi2_tool_widgets = {},
  i2b2, $j, alert));
