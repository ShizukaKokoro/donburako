//! Donburako
//!
//! データ駆動のワークフローを作成するためのライブラリ。
//! ワークフローは、ノードとエッジから構成される有向グラフで表現される。
//! ノードは、ワークフローのステップを表し、エッジはノード間の依存関係を表す。
//! 依存関係のないノードは並列に実行されるため、ワークフローの実行は効率的に行われる。
//!
//! # 概要
//!
//! ## イメージ
//!
//! 桃太郎が桃の中に入った状態で、川を流れているように、データをコンテナに入れて、ワークフローを流すイメージ。
//!
//! ## コンポーネント
//!
//! ## コンテナ
//!
//! データを格納するためのコンテナ。
//! 中にデータが入っており、エッジを通ってノードに渡される。
//! コンテナは、プロセッサーに入れられ、次に通るべきエッジをノードから指定されて、次のノードに渡される。
//! コンテナは、ノードごとに生成・破棄されるのではなく、基本的に複製され、データの統合の際と、ワークフローの終了時に破棄される。
//!
//! ### ノード
//!
//! 実行されるタスクを表す。
//! 非同期に実行されるため、 IO や通信などの、 CPU バウンドでない処理を効率的に並列実行できる。
//! CPU バウンドかどうかを判断でき、バウンドしている場合は、ブロッキングスレッドを生成して並列実行する。そうでない場合は、非同期タスクとして実行する。
//! 実行可能になると、ノードはキューに追加され、最大の並列実行数を超えないように実行される。
//! 全てのノードのハンドルを `select!` マクロで一定時間ごとにチェックすることで、ノードの実行状況を監視できる。
//! ノードと外部のデータのやり取りは、ノード内部で行う。
//! ノード内部で入力を受け取るまで待機し、全ての入力を受け取ったら、処理を開始する。
//! 処理が完了したら全ての出力を返す。レジストリには何も入力されず、空になる。
//!
//! ### エッジ
//!
//! ノード間の依存関係を表す。
//! 一つのノードが複数のエッジを前にも後にも持つことができる。これにより、複数の引数と複数の戻り値を扱うことができる。
//! それぞれ始点と終点が同じかつ、データ型も同じようなエッジを許容する。
//!
//! ### ワークフロー
//!
//! ノードとエッジの集合。
//! どのエッジがどのノードに接続されているかも含まれる。
//! ワークフローが実行される形になる。
//! ワークフローは同時に実行されうる。
//!
//! ### プロセッサー
//!
//! ワークフローを実行するためのエンジン。
//! ワークフローを登録し、任意のタイミングで実行する。
//! ワークフローに対して、コンテナを生成し、最初のエッジのプロセッサーマップにデータを格納する。
//! エッジのマップにデータが格納されると、そのエッジに接続されたノードが実行される。
//! ノードが実行されるとノードは出力エッジを参照して、エッジのプロセッサーマップにコンテナを格納する。
//! プロセッサーマップは、コンテナを格納し、コンテナの数を確認できるようにする。
//!
//! ## 使い方
//!
//! ### セットアップ
//!
//! 1. ノードを用いて、ノードとエッジを作成する。(エッジは後で用いるため、保管しておく)
//! 2. ワークフローを作成し、ノードを登録する。(エッジは自動的に登録される)
//! 3. プロセッサーを作成し、ワークフローを登録する。
//! 4. 1~3 を繰り返し、複数のワークフローを登録する。
//!
//! ### 実行開始
//!
//! 1. プロセッサーを起動する。コンテナマップが生成される。
//! 2. ワークフローを指定して、実行準備状態にする。(実行IDが生成される)
//! 3. コンテナマップに実行IDを指定してデータをエッジに格納する。(格納されたことをプロセッサーに通知する)
//! 4. プロセッサーがエッジに格納されたデータを確認し、実行できるノードを実行する。
//! 5. ノードが実行されると、最後にどこにも接続されていないエッジにデータが格納される。
//! 6. ワークフローチャンネルがユーザーに実行の完了を通知し、最後のエッジに格納されたデータを取得する。
//! 7. 2~6 を非同期に繰り返す。(同時に複数のワークフローを実行できる)
//!
//! ### シャットダウン
//!
//! 1. コンテナに保管されているデータを全て破棄する。
//! 2. プロセッサーをストップする。
//! 3. プロセッサーのハンドルが終了するのを待つ。
//!
//! # 非目標
//!
//! エッジからコンテナが取り出されたあと、同じ実行IDで再度エッジにデータを格納することができ、これによりデータの整合性が崩れるが、それを阻止する仕組みは実装せず、ユーザー側で対応する必要がある。

#![warn(missing_docs, rustdoc::missing_crate_level_docs, unused_results)]

pub mod channel;
pub mod container;
pub mod edge;
pub mod node;
pub mod operator;
pub mod processor;
pub mod workflow;

pub use crate::channel::workflow_channel;
pub use crate::node::NodeBuilder;
pub use crate::processor::ProcessorBuilder;
pub use fake::{Fake, Faker};
