const TerserPlugin = require('terser-webpack-plugin')

module.exports = {
    cache: {
        type: 'filesystem',
    },
    entry: {
        index: __dirname + '/index.jsx',
        query: __dirname + '/query.jsx',
        plan: __dirname + '/plan.jsx',
        embedded_plan: __dirname + '/embedded_plan.jsx',
        references: __dirname + '/references.jsx',
        stage: __dirname + '/stage.jsx',
        worker: __dirname + '/worker.jsx',
        workers: __dirname + '/workers.jsx',
        timeline: __dirname + '/timeline.jsx',
    },
    mode: 'production',
    module: {
        rules: [
            {
                test: /\.(js|jsx)$/,
                exclude: /node_modules/,
                use: ['babel-loader'],
            },
        ],
    },
    resolve: {
        extensions: ['*', '.js', '.jsx'],
    },
    output: {
        path: __dirname + '/../dist',
        filename: '[name].js',
    },
    optimization: {
        minimize: true,
        chunkIds: 'deterministic',
        minimizer: [
            new TerserPlugin({
                terserOptions: {
                    format: {
                        comments: false,
                    },
                },
                extractComments: false,
            }),
        ],
    },
}
