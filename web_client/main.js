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
