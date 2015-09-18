{
    files: [
        'tool_widgets.js',
        'chi2_ctrlr.js'],
    css: ['tools.css'],
    config: {
        short_name: 'Chi2',
        name: 'Chi-squared, topN over/under represented facts',
        description: ('Chi-squared topN over/under represented facts in a cohort'),
        category: ["celless", "plugin", "uthscsa"],
        plugin: {
            isolateHtml: false,
            isolateComm: true,      // this means to expect the plugin to use AJAX communications provided by the framework
            standardTabs: true, // this means the plugin uses standard tabs at top
            html: {
                source: 'chi2_ui.html',
                mainDivId: 'analysis-mainDiv'
            }
        }
    }
}

