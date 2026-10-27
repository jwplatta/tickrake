# frozen_string_literal: true

require "aws-sdk-s3"
require "pathname"

module Tickrake
  module Storage
    class ObjectStoreClient
      RemoteObject = Struct.new(:bucket, :key, :size, keyword_init: true) do
        def uri
          "s3://#{bucket}/#{key}"
        end
      end

      def initialize(config, datastore_config: nil, target_config: nil, s3_client: nil)
        @config = config
        @target_config = target_config || datastore_config || (config.respond_to?(:staging_target) ? config.staging_target : nil)
        raise Tickrake::Error, "Object store target is not configured." unless @target_config

        @s3_client = s3_client
      end

      def key_for(local_path)
        @target_config.prefixed_key(relative_path_for(local_path))
      end

      def uri_for(local_path)
        "s3://#{@target_config.bucket}/#{key_for(local_path)}"
      end

      def upload(local_path)
        absolute_path = Tickrake::PathSupport.expand_path(local_path)
        remote_object = remote_object_for(absolute_path)

        File.open(absolute_path, "rb") do |body|
          s3_client.put_object(
            **put_params(bucket: remote_object.bucket, key: remote_object.key, body: body)
          )
        end

        remote_object
      end

      def bucket
        @target_config.bucket
      end

      def upload_content(key, content)
        s3_client.put_object(
          **put_params(bucket: @target_config.bucket, key: key, body: content)
        )
        RemoteObject.new(bucket: @target_config.bucket, key: key, size: content.bytesize)
      end

      def download_content(key)
        response = s3_client.get_object(bucket: @target_config.bucket, key: key)
        response.body.read
      end

      def upload_file(local_path, key:)
        absolute_path = Tickrake::PathSupport.expand_path(local_path)
        File.open(absolute_path, "rb") do |body|
          s3_client.put_object(
            **put_params(bucket: @target_config.bucket, key: key, body: body)
          )
        end
        RemoteObject.new(bucket: @target_config.bucket, key: key, size: File.size(absolute_path))
      end

      def object_exists?(key)
        s3_client.head_object(bucket: @target_config.bucket, key: key)
        true
      rescue Aws::S3::Errors::NotFound
        false
      end

      def list_keys(prefix:)
        keys = []
        continuation_token = nil
        loop do
          resp = s3_client.list_objects_v2(
            bucket: @target_config.bucket,
            prefix: prefix,
            continuation_token: continuation_token
          )
          keys.concat(resp.contents.map(&:key))
          break unless resp.is_truncated

          continuation_token = resp.next_continuation_token
        end
        keys
      end

      def delete_keys(keys)
        return if keys.empty?

        keys.each_slice(1000) do |batch|
          s3_client.delete_objects(
            bucket: @target_config.bucket,
            delete: { objects: batch.map { |k| { key: k } }, quiet: true }
          )
        end
      end

      def verify(local_path)
        absolute_path = Tickrake::PathSupport.expand_path(local_path)
        remote_object = remote_object_for(absolute_path)
        response = s3_client.head_object(bucket: remote_object.bucket, key: remote_object.key)
        RemoteObject.new(bucket: remote_object.bucket, key: remote_object.key, size: response.content_length)
      end

      private

      def put_params(bucket:, key:, body:)
        params = {
          bucket: bucket,
          key: key,
          body: body
        }
        params[:storage_class] = @target_config.storage_class if @target_config.storage_class
        params
      end

      def remote_object_for(local_path)
        RemoteObject.new(bucket: @target_config.bucket, key: key_for(local_path), size: File.size(local_path))
      end

      def relative_path_for(local_path)
        expanded_data_dir = Tickrake::PathSupport.expand_path(@config.data_dir)
        absolute_path = Tickrake::PathSupport.expand_path(local_path)
        relative = Pathname.new(absolute_path).relative_path_from(Pathname.new(expanded_data_dir)).to_s
        raise Tickrake::Error, "Archive path must stay within #{expanded_data_dir}: #{absolute_path}" if relative.start_with?("..")

        relative.split(File::SEPARATOR).join("/")
      rescue ArgumentError
        raise Tickrake::Error, "Archive path must stay within #{expanded_data_dir}: #{absolute_path}"
      end

      def s3_client
        @s3_client ||= begin
          options = {}
          options[:region] = @target_config.region if @target_config.region
          if @target_config.respond_to?(:endpoint) && @target_config.endpoint
            options[:endpoint] = @target_config.endpoint
          end
          if @target_config.respond_to?(:force_path_style) && @target_config.force_path_style
            options[:force_path_style] = true
          end
          if @target_config.respond_to?(:access_key_id) && @target_config.access_key_id
            options[:credentials] = Aws::Credentials.new(
              @target_config.access_key_id,
              @target_config.secret_access_key
            )
          end
          Aws::S3::Client.new(**options)
        end
      end
    end

    # Backward compatibility alias
    S3Archive = ObjectStoreClient
  end
end
