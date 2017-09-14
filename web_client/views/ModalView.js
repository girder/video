import $ from 'jquery';
import _ from 'underscore';
import reconcile from 'reconcile.js';
import View from 'girder/views/View';
import { restRequest } from 'girder/rest';
import 'girder/utilities/jquery/girderModal';

import VideoWidget from './VideoWidget';

import template from '../templates/ModalView.pug';

var ModalView = View.extend({
    events: {
        
    },
    initialize(settings) {
        this.fileModel = settings.model;
    },
    render() {
        this.$el.html(template({})).girderModal(this)
        .on('shown.bs.modal', () => {
            new VideoWidget({
                className: 'g-file-video',
                model: this.fileModel,
                parentView: this,
                modalMode: true
            })
            .render()
            .$el.appendTo(this.$('.modal-body'));
        })
        .on('hidden.bs.modal', () => {
            
        });
        return this;
    }
});

export default ModalView;