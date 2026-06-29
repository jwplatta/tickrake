# frozen_string_literal: true

require "aws-sdk-s3"

module Tickrake
  module Storage
    class S3Archive
      RemoteObject = Struct.new(:bucket, :key, :size, keyword_init: true) do
        def uri
          "s3://#{bucket}/#{key}"
        end
      end

      def initialize(config, archive_config: config.s3_archive, s3_client: nil)
        @config = config
        @archive_config = archive_config
        raise Tickrake::Error, "S3 archive is not configured." unless @archive_config

        @s3_client = s3_client
      end

      def key_for(local_path)
        @archive_config.prefixed_key(relative_path_for(local_path))
      end

      def uri_for(local_path)
        "s3://#{@archive_config.bucket}/#{key_for(local_path)}"
      end

      def upload(local_path)
        absolute_path = Tickrake::PathSupport.expand_path(local_path)
        remote_object = remote_object_for(absolute_path)

        File.open(absolute_path, "rb") do |body|
          s3_client.put_object(
            bucket: remote_object.bucket,
            key: remote_object.key,
            body: body,
            storage_class: @archive_config.storage_class
          )
        end

        remote_object
      end

      def verify(local_path)
        absolute_path = Tickrake::PathSupport.expand_path(local_path)
        remote_object = remote_object_for(absolute_path)
        response = s3_client.head_object(bucket: remote_object.bucket, key: remote_object.key)
        RemoteObject.new(bucket: remote_object.bucket, key: remote_object.key, size: response.content_length)
      end

      private

      def remote_object_for(local_path)
        RemoteObject.new(bucket: @archive_config.bucket, key: key_for(local_path), size: File.size(local_path))
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
          options[:region] = @archive_config.region if @archive_config.region
          Aws::S3::Client.new(**options)
        end
      end
    end
  end
end
