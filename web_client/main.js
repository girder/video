import $ from 'jquery';
import _ from 'underscore';
import { wrap } from 'girder/utilities/PluginUtils';
import ItemView from 'girder/views/body/ItemView';
import FileListWidget from 'girder/views/widgets/FileListWidget';

import ItemVideosWidget from './views/ItemVideosWidget';

wrap(FileListWidget, 'render', function (render) {
    render.call(this);
    var videoFiles = this.collection.filter((file) => {
        return file.get('mimeType') === 'video/mp4';
    });
    if (videoFiles.length > 0) {
        new ItemVideosWidget({
            className: 'g-item-videos',
            collection: videoFiles,
            parentView: this
        })
        .render()
        .$el.appendTo(this.$el);
    }
});

// wrap(ItemView, 'render', function (render) {
//     // ItemView is a special case in which rendering is done asynchronously,
//     // so we must listen for a render event.

//     var videoFiles = this.model.filter((file) => {
//         return file.get('mimeType') === 'video/mp4';
//     });
//     if (videoFiles.length > 0) {
        
//     }
    
//     this.once('g:rendered', function () {
//         var itemLicenseItemWidget = new ItemVideoWidget({
//             className: 'g-item-video',
//             item: this.model,
//             parentView: this
//         }).render()
//             .$el.appendTo(this.$el);

//         // $('.g-item-info').append(itemLicenseItemWidget.el);
//     }, this);

//     render.call(this);

//     return this;
// });