import $ from 'jquery';
import _ from 'underscore';
import View from 'girder/views/View';

import VideoWidget from './VideoWidget';
import template from '../templates/ItemVideosWidget.pug';

import '../stylesheets/ItemVideosWidget.styl';

var ItemVideosWidget = View.extend({
    events: {

    },
    initialize(settings) {
        this.collection = settings.collection;
    },
    render() {
        this.$el.html(template());
        this.collection.forEach((file) => {
            new VideoWidget({
                className: 'g-file-video',
                model: file,
                parentView: this
            })
            .render()
            .$el.appendTo(this.$('.g-item-video-content'));
        });
        return this;
    }
});

export default ItemVideosWidget;