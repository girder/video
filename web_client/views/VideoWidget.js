import $ from 'jquery';
import _ from 'underscore';
import View from 'girder/views/View';
import { getApiRoot, restRequest } from 'girder/rest';

import template from '../templates/VideoWidget.pug';

var VideoWidget = View.extend({
    events: {

    },
    initialize(settings) {
        this.model = settings.model;
    },
    render() {
        this.$el.html($(template({ src: `${getApiRoot()}/file/${this.model.id}/download` })));
        return this;
    }
});

export default VideoWidget;