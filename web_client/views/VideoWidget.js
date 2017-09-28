import $ from 'jquery';
import _ from 'underscore';
import * as reconcile from 'reconcile.js';
import View from 'girder/views/View';
import { getApiRoot, restRequest } from 'girder/rest';

import ModalView from './ModalView';

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
            }, 3000);
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
        },
        'click button.popup': function (e) {
            new ModalView({
                el: $('#g-dialog-container'),
                model: this.fileModel,
                parentView: this
            }).render();
        }
    },
    initialize(settings) {
        this.fileModel = settings.model;
        this.modalMode = settings.modalMode;
        this.buildInControl = !settings.noBuildInControl;
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
            }, 3000);
        }
    },
    render() {
        let getTemplate = () => {
            return template(Object.assign({ src: `${getApiRoot()}/file/${this.fileModel.id}/download` }, this));
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
    // this function updates the DOM instead of destroy and recreate it with reconcile.js, which helps perserve the state of <video>
    update(template) {
        var $new = $(template);
        var changes = reconcile.diff($new[0], this.$el.children(0)[0]);
        reconcile.apply(changes, this.$el.children(0)[0]);
    }
});

export default VideoWidget;