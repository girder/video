import $ from 'jquery';
import _ from 'underscore';
import reconcile from 'reconcile.js';
import View from 'girder/views/View';
import { getApiRoot, restRequest } from 'girder/rest';

import template from '../templates/VideoWidget.pug';

import '../stylesheets/VideoWidget.styl';

var VideoWidget = View.extend({
    events: {
        'click button.convert': function (e) {
            this.status = 2;
            this.playing = false;
            this.render();
            setTimeout(() => {
                this.status = 3;
                this.render();
            }, 5000);
        },
        'click button.delete': function (e) {
            this.status = 0;
            this.playing = false;
            this.render();
        },
        'click button.play': function (e) {
            this.videoElement.play();
        },
        'click button.pause': function (e) {
            this.videoElement.pause();
        }
    },
    initialize(settings) {
        this.model = settings.model;
        this.status = (() => {
            let status = Math.floor(Math.random() * 3);
            if (status !== 0) {
                status += 1;
            }
            return status;
        })();
        if (this.status === 2) {
            setTimeout(() => {
                this.status = 3;
                this.render();
            }, 5000);
        }
    },
    render() {
        let getTemplate = () => {
            return template(Object.assign({ src: `${getApiRoot()}/file/${this.model.id}/download` }, this));
        };
        if (!this.initialized) {
            this.initialized = true;
            this.$el.html(getTemplate());
        }
        else {
            this.update(getTemplate());
        }
        if (this.videoElement) {
            $(this.videoElement).off();
        }
        this.videoElement = this.$('.g-video-preview')[0];
        this.$('.g-video-preview').on('play', () => {
            this.playing = true;
            this.render();
        });
        this.$('.g-video-preview').on('pause', () => {
            this.playing = false;
            this.render();
        });
        return this;
    },
    update(template) {
        var $new = $(template);
        var changes = reconcile.diff($new[0], this.$el.children(0)[0]);
        reconcile.apply(changes, this.$el.children(0)[0]);
    }
});

export default VideoWidget;